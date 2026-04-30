"""PIM -> Magento sync (v2).

Pipeline
--------
  1. Poll pim.alumio_sync_queue for product_ids with status in ('pending', 'failed').
  2. Fetch full product details + related data (attributes, images, prices, stock,
     categories, team/season metadata, store/website assignments).
  3. Transform into scope-correct Magento payload entries.
  4. POST to Alumio webhook in batches; mark queue rows 'success' on success,
     'failed' on failure (rows are retained for audit, not deleted).
  5. Housekeeping: delete 'success' rows older than 30 days.

Scope model
-----------
Magento has three attribute scopes: Global, Website, Store View. Each PIM
product is emitted as one payload entry per store, carrying:

    store_view_codes: [store_code]        -- routes store-view attributes
    websites:         [website_code]      -- routes website-scoped attributes
    price, special_price, cost            -- website-scoped, read from
                                             pim.product_price for this store
    name, description, images             -- store-view-scoped
    product_type, visibility, url_key ... -- global (Magento auto-routes)

Alumio / Magento accept this single consolidated shape: a 200 response means
the payload was received, and Magento routes each field to its correct scope.
If a website-scoped field doesn't appear in Magento despite a 200 response,
the cause is upstream of Magento (usually an attribute missing from Alumio's
outbound product mapping) -- it is NOT a scope-routing bug in this script.

Change log (most recent first)
------------------------------
2026-04-23 (late-late)  Revert scope-split; confirm Alumio mapping gap.
  Backup of the pre-change file:
      sync/backups/pim_to_magento_sync_v2_BACKUP_20260423_pre-unsplit.py

  REVERTED - the store-view/website payload split introduced earlier today.
    Hypothesis that motivated the split: Magento silently drops website-scoped
    price fields when shipped alongside store_view_codes.
    Evidence that disproved it: even after splitting, special_price and cost
    still did not land in Magento. Furthermore, Josh confirmed these two
    fields have NEVER landed for any product via this pipeline, while `price`
    always does. Alumio's middleware viewer also showed two entries per SKU
    (indexed 0 and 1), which a colleague in tech flagged as unusual input.
    Conclusion: the scope-split was not solving a real problem; the real bug
    is in Alumio's outbound Magento attribute mapping (special_price and cost
    look to be missing from the mapping whitelist, or their transformer is
    broken).

  Changes:
      * Removed _split_for_scope_correctness() and its call site.
      * Removed WEBSITE_SCOPED_PRODUCT_KEYS, WEBSITE_AND_GLOBAL_PRODUCT_KEYS.
      * Removed _websites_for_store_codes() (only used by the split helper).
      * Removed `copy` import (only used by the split helper).
      * transform_to_magento() now emits one consolidated scoped payload per
        (product_id, store) -- store_view_codes, websites, price,
        special_price, cost and all other fields on the same entry.

  Not changed: the per-store price fix below. That remains correct and is
  independent of the split.

2026-04-23 (late)  Per-store price lookup.
  Backup of the pre-change file:
      sync/backups/pim_to_magento_sync_v2_BACKUP_20260423_pre-price-fix.py

  FIXED - prices now resolved by (product_id, store_id), not by currency.
    Root cause: pim.product_price is keyed on (product_id, store_id) with
    currency carried per-row. The script was joining on (product_id,
    currency_id) and looking up prices by a store->currency map, so any
    store-specific price override was ignored. For the UK rollout this
    manifested as the base store receiving the generic GBP row rather than
    its own priced row.
    Confirmed with Josh 2026-04-23: store_id is never NULL in product_price
    and the correct key is (product_id, store_id).

  Changes:
      * New get_prices_per_store(): returns {product_id: {store_id: info}}.
      * _PRODUCT_DETAIL_QUERY: price join removed (prices fetched separately).
      * get_store_assignments_for_products(): now returns store_to_store_id_map.
      * transform_to_magento(): iterates per-store, not per-currency. Each
        store gets its own scoped payload with its own price.
      * _build_scoped_product(): takes prices_for_store (single store) +
        store_id, instead of variant_prices_map (per-currency).
      * get_store_currency_map() retained as a utility but no longer called
        in the main pipeline.

2026-04-23 (early)  Heavy tidy-up + price-scope fix.
  Backup of the pre-change file:
      sync/backups/pim_to_magento_sync_v2_BACKUP_20260423_pre-tidy.py

  REMOVED - sparse scope fragmentation
      * ALUMIO_SPARSE_SCOPE_PAYLOADS toggle (was always False in prod)
      * build_scope_fragments()
      * _bucket_custom_attributes(), _normalize_magento_scope()
      * _aggregate_websites_for_scoped_products()
      * _estimate_payload_bytes()
    These supported an optional-fragment mode that never shipped. The correct
    scope-splitting is now the *default* behaviour, not an opt-in.

  REMOVED - create-vs-update delta payloads
      * _sync_mode ('create' / 'update') tagging on rows
      * sync_mode_map param on get_updated_products() / get_product_details()
      * _build_update_products()
      * _strip_create_only_custom_attributes()
      * CREATE_ONLY_PRODUCT_KEYS / CREATE_ONLY_CUSTOM_ATTRIBUTE_CODES
    Every queued product_id now sends a full payload. No minimal-update deltas.

  REMOVED - dead v1 code
      * transform_to_magento_current() (unused legacy path)
      * SIMPLE_HARDCODED_ATTRIBUTES / CONFIGURABLE_HARDCODED_ATTRIBUTES
        aliases (only transform_to_magento_current referenced them)
      * _to_global_scope_product() (its job is now done by the store-view
        entry + website entry split)

  FIXED - price / special_price / cost now land in Magento
    Root cause: these fields were shipped in payloads that also carried
    `store_view_codes`. Magento treats them as Website-scoped, and silently
    drops them when posted with a store-view hint.
    Observed with TM405-227-M: special_price=24.99 was in the Alumio payload,
    Alumio returned 200 / "success", but the Magento admin grid still showed
    Special Price empty.
    Fix: each per-scope payload is now emitted as TWO entries via
    _split_for_scope_correctness() -- a store-view entry (no price fields)
    and a separate website-scoped entry carrying the price fields.

  Restructured into nine top-level sections (see banner comments).
"""

# =============================================================================
# SECTION 1: IMPORTS & ENVIRONMENT
# =============================================================================

import json
import os
import time
from datetime import datetime
from decimal import Decimal

import psycopg2
import requests
from google.cloud import bigquery

os.environ["GCLOUD_PROJECT"] = "classic-football-shirts"
credential_path = "/Users/joshcolclough/Documents/classic-football-shirts-975adab4045c.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path

DB_HOST = os.environ.get("PIM_POSTGRES_HOST")
DB_NAME = os.environ.get("PIM_POSTGRES_NAME")
DB_USER = os.environ.get("PIM_POSTGRES_USER")
DB_PASSWORD = os.environ.get("PIM_POSTGRES_PASSWORD")
ALUMIO_WEBHOOK_URL = os.environ.get("ALUMIO_PRODUCT_UPSERT_WEBHOOK_URL")
MAX_PRODUCT_IDS_PER_BATCH = int(os.environ.get("ALUMIO_BATCH_SIZE", "10"))

# Website allowlist
# ─────────────────
# Comma-separated list of Magento website codes this sync is allowed to send to.
# Products not assigned to at least one allowed website are skipped (queue row
# left untouched so it retries once the website is added to the allowlist).
# Default "base" = UK-only while we prove out v2. Expand to "base,eu,us" once
# ready to roll out to other storefronts.
ALLOWED_WEBSITE_CODES = [
    c.strip().lower()
    for c in os.environ.get("ALLOWED_WEBSITE_CODES", "base").split(",")
    if c.strip()
]

ALUMIO_PAYLOAD_LOG_PATH = os.environ.get(
    "ALUMIO_PAYLOAD_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "alumio_payloads.jsonl"),
)

_required_vars = {
    "PIM_POSTGRES_HOST": DB_HOST,
    "PIM_POSTGRES_NAME": DB_NAME,
    "PIM_POSTGRES_USER": DB_USER,
    "PIM_POSTGRES_PASSWORD": DB_PASSWORD,
    "ALUMIO_PRODUCT_UPSERT_WEBHOOK_URL": ALUMIO_WEBHOOK_URL,
}
_missing = [k for k, v in _required_vars.items() if not v]
if _missing:
    raise Exception(f"Missing required environment variables: {', '.join(_missing)}")

bq_client = bigquery.Client()


# =============================================================================
# SECTION 2: CONFIGURATION CONSTANTS
# =============================================================================

# ── Scope model ──────────────────────────────────────────────────────────────
# ── Queue SQL filter ─────────────────────────────────────────────────────────
CATEGORY_OPERATION_SQL = (
    "LOWER(REPLACE(COALESCE(operation_type, ''), ' ', '_')) = 'category_change'"
)

# ── Hardcoded attributes shared by ALL product types ─────────────────────────
_COMMON_HARDCODED_ATTRIBUTES = {
    # Stock configuration
    "use_config_min_qty": 1,
    "is_qty_decimal": 0,
    "allow_backorders": 0,
    "use_config_backorders": 1,
    "min_cart_qty": 1,
    "use_config_min_sale_qty": 1,
    "max_cart_qty": 10000,
    "use_config_max_sale_qty": 1,
    "notify_on_stock_below": 1,
    "use_config_notify_stock_qty": 1,
    "manage_stock": 1,
    "use_config_manage_stock": 1,
    "use_config_qty_increments": 1,
    "qty_increments": 1,
    "is_decimal_divided": 0,
    # Common display / SEO
    "gift_message_available": "No",
    "googlecode": 5564,
    "in_xml_sitemap": "Yes",
}

# Classic standalone simple products (CLASSIC IMPORT sheet)
CLASSIC_HARDCODED_ATTRIBUTES = {
    **_COMMON_HARDCODED_ATTRIBUTES,
    "display_product_options_in": "Block after Info Column",
    "content_attributes_enabled": 1,
    "description_exact_image": 1,
    "out_of_stock_qty": 0,
    "use_config_enable_qty_inc": 1,
    "enable_qty_increments": 0,
    "fbfeedyesono": "No",
    "feedmodelshots": "no_selection",
    "mst_search_weight": 0,
    "usedfeed": "No",
    "amazonyesno": "No",
    "rw_google_base_skip_submi": "No",
    "price_view": "Price Range",
}

# BNIB simple children (NEW listing - Import1 sheet)
BNIB_SIMPLE_HARDCODED_ATTRIBUTES = {
    **_COMMON_HARDCODED_ATTRIBUTES,
    "use_config_enable_qty_inc": 0,
    "enable_qty_increments": 1,
    "fbfeedyesono": "Yes",
    "coa": "No",
}

# BNIB configurable parents (NEW listing - Import2 sheet)
BNIB_CONFIGURABLE_HARDCODED_ATTRIBUTES = {
    **BNIB_SIMPLE_HARDCODED_ATTRIBUTES,
    "description_size_guide_enabled": "Yes",
    "productcomment": 0,
}

# Populated at runtime from pim.attribute_def where magento_sync = false
DISABLED_HARDCODED_ATTRIBUTE_CODES = set()

# ── Attribute exclusion lists ────────────────────────────────────────────────
# Attributes stripped from all payloads just before send (kept in the hardcoded
# dicts above for reference but not shipped to Magento).
SKIPPED_ATTRIBUTE_CODES = {
    "display_product_options_in",
    "qty_increments",
    "use_config_enable_qty_inc",
    "enable_qty_increments",
    "bnib",
    "usedfeed",
    "fbfeedyesono",
}

# Attributes excluded from specific store-view payloads. Set via the global
# scope and must not be re-sent at store-view level where the option values
# may not exist yet.
STORE_VIEW_EXCLUDED_ATTRIBUTES = {
    "eu": {"brand_n"},
}

# Attributes that should only be sent on visible products (visibility != 1).
# description_notes shouldn't appear on "Not Visible Individually" products.
_VISIBLE_ONLY_ATTRIBUTE_CODES = {"description_notes"}


# =============================================================================
# SECTION 3: LOGGING
# =============================================================================

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# =============================================================================
# SECTION 4: QUEUE MANAGEMENT
# =============================================================================

def get_last_sync_time():
    """Return the ISO timestamp of the previous successful sync (BigQuery)."""
    try:
        query = """
            SELECT MAX(last_sync_time) as last_sync
            FROM `classic-football-shirts.pim.sync_status`
        """
        results = list(bq_client.query(query).result())
        if not results or results[0][0] is None:
            log("No previous sync found, starting from 2000-01-01")
            return "2000-01-01 00:00:00"
        return results[0][0].strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        log(f"Failed to query BigQuery sync status: {e}")
        raise


def get_updated_products(since):
    """Return the list of product_ids queued for sync.

    CHANGED 2026-04-23: previously returned (product_id, sync_mode) tuples so
    creates could be routed to a full payload and updates to a minimal delta.
    That distinction has been removed -- every queued row now sends a full
    payload -- so we only need the product_ids.

    Picks up both 'pending' (new) and 'failed' (retry) rows, so transient
    failures are retried automatically on the next run.
    """
    try:
        log(f"Connecting to PIM database: {DB_HOST}:{DB_NAME}")
        conn = _pg_connect()
        query = f"""
            SELECT product_id, MIN(created_at) AS first_created
            FROM pim.alumio_sync_queue
            WHERE status IN ('pending', 'failed')
              AND NOT ({CATEGORY_OPERATION_SQL})
            GROUP BY product_id
            ORDER BY MIN(created_at)
        """
        cursor = conn.cursor()
        cursor.execute(query)
        product_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return product_ids
    except Exception as e:
        log(f"Error querying PIM for queued products: {e}")
        raise


def update_alumio_sync_queue(success_product_ids, failed_product_ids):
    """Mark processed queue rows with status='success' or status='failed'.

    CHANGED 2026-04-28: previously DELETED successful rows. Now retains them
    with status='success' for audit history; old success rows are removed by
    cleanup_old_success_rows() on a 30-day retention. Both branches scope
    updates to rows currently in 'pending' or 'failed' state, so a 'failed'
    row that succeeds on a later retry transitions to 'success'.
    """
    try:
        if not success_product_ids and not failed_product_ids:
            return

        log("Updating Alumio sync queue")
        conn = _pg_connect()
        cursor = conn.cursor()
        succeeded = 0
        failed = 0

        if success_product_ids:
            cursor.execute(
                f"""
                UPDATE pim.alumio_sync_queue
                SET status = 'success', processed_at = NOW()
                WHERE status IN ('pending', 'failed')
                  AND product_id = ANY(%s)
                  AND NOT ({CATEGORY_OPERATION_SQL})
                """,
                (list(success_product_ids),),
            )
            succeeded = cursor.rowcount

        if failed_product_ids:
            cursor.execute(
                f"""
                UPDATE pim.alumio_sync_queue
                SET status = 'failed', processed_at = NOW()
                WHERE status IN ('pending', 'failed')
                  AND product_id = ANY(%s)
                  AND NOT ({CATEGORY_OPERATION_SQL})
                """,
                (list(failed_product_ids),),
            )
            failed = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        if succeeded:
            log(f"Marked {succeeded} queued change(s) as success")
        if failed:
            log(f"Marked {failed} queued change(s) as failed")
    except Exception as e:
        log(f"Error updating Alumio sync queue: {e}")
        raise


def cleanup_old_success_rows(retention_days=30):
    """Delete 'success' queue rows older than `retention_days` days.

    Run at the end of each sync to keep the queue table bounded. Only rows
    with status='success' are removed -- 'failed' rows are kept so they can
    be retried or audited indefinitely. Age is measured against processed_at
    (falling back to created_at for older rows that pre-date this column
    being populated).
    """
    try:
        conn = _pg_connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM pim.alumio_sync_queue
            WHERE status = 'success'
              AND COALESCE(processed_at, created_at) < NOW() - (%s || ' days')::interval
            """,
            (str(retention_days),),
        )
        deleted = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        if deleted:
            log(f"Cleanup: deleted {deleted} 'success' row(s) older than {retention_days} days")
    except Exception as e:
        # Cleanup is best-effort; don't fail the whole sync if it errors.
        log(f"WARNING: cleanup of old success rows failed (non-fatal): {e}")


def update_sync_status(product_count, success_count):
    """Record the current sync time + counts to BigQuery."""
    try:
        query = """
            INSERT INTO `classic-football-shirts.pim.sync_status`
            (sync_time, last_sync_time, products_synced, products_failed)
            VALUES (CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @success, @failed)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("success", "INT64", success_count),
                bigquery.ScalarQueryParameter(
                    "failed", "INT64", product_count - success_count
                ),
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        log(
            f"Updated sync status in BigQuery: {success_count}/{product_count} products"
        )
    except Exception as e:
        log(f"Error updating sync status in BigQuery: {e}")
        raise


# =============================================================================
# SECTION 5: DATA FETCH
# =============================================================================

def _pg_connect():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432,
    )


def get_store_assignments_for_products(product_ids, conn):
    """Return map of product_id -> {store_view_codes, website_codes,
    store_to_website_map, store_to_store_id_map}.

    CHANGED 2026-04-23 (pre-price-fix): now also returns store_to_store_id_map
    so downstream price lookup can resolve store_code -> store_id. Prices are
    keyed by (product_id, store_id) in pim.product_price, so the transform
    needs the numeric store_id for each store_code it's emitting a payload for.

    If ALLOWED_WEBSITE_CODES is set, store rows whose website_code isn't in
    the allowlist are filtered out at SQL level. Downstream logic (price
    lookup, scope-aware attribute lookup, payload emission) all key off
    store_view_codes, so this one filter cascades through the pipeline.
    """
    if not product_ids:
        return {}
    try:
        query = """
            SELECT ps.product_id, s.store_id, s.code, s.website_code
            FROM pim.product_store ps
            JOIN pim.store s ON ps.store_id = s.store_id
            WHERE ps.product_id = ANY(%s) AND ps.enabled = true
        """
        params = [product_ids]
        if ALLOWED_WEBSITE_CODES:
            query += "\n              AND LOWER(s.website_code) = ANY(%s)"
            params.append(ALLOWED_WEBSITE_CODES)

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        assignments = {}
        for product_id, store_id, store_code, website_code in cursor.fetchall():
            entry = assignments.setdefault(
                product_id,
                {
                    "store_view_codes": [],
                    "website_codes": [],
                    "store_to_website_map": {},
                    "store_to_store_id_map": {},
                },
            )
            if store_code:
                sc = store_code.lower()
                if sc not in entry["store_view_codes"]:
                    entry["store_view_codes"].append(sc)
                if website_code:
                    entry["store_to_website_map"][sc] = website_code.lower()
                if store_id is not None:
                    entry["store_to_store_id_map"][sc] = int(store_id)
            if website_code:
                wc = website_code.lower()
                if wc not in entry["website_codes"]:
                    entry["website_codes"].append(wc)
        cursor.close()
        return assignments
    except Exception as e:
        log(
            f"Warning: Error fetching store assignments for {len(product_ids)} products: {e}"
            " - will continue without websites"
        )
        return {}


def get_images_for_products(product_ids, conn):
    """Return map of product_id -> list of image dicts shaped for Magento.

    For configurable parents, all child images come through the parent's
    product_id (see caller in get_product_details).
    """
    if not product_ids:
        return {}
    try:
        query = """
            SELECT product_id, role, url, label, sort_order
            FROM pim.image_asset
            WHERE product_id = ANY(%s)
            ORDER BY product_id, sort_order, image_id
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_ids,))

        images_by_product = {}
        role_map = {
            "base": "image",
            "small": "small_image",
            "thumbnail": "thumbnail",
            "additional": "gallery",
            "swatch": "swatch",
        }
        for product_id, role, url, label, sort_order in cursor.fetchall():
            if not url:
                continue
            file_path = str(url).strip()
            if not file_path:
                continue
            per_product = images_by_product.setdefault(product_id, {})
            image = per_product.get(file_path)
            if not image:
                image = {
                    "file": file_path,
                    "media_type": "image",
                    "types": [],
                }
                if label:
                    image["label"] = label
                if sort_order is not None:
                    image["position"] = max(int(sort_order) - 1, 0)
                per_product[file_path] = image
            if role:
                mapped = role_map.get(role.lower())
                if mapped and mapped not in image["types"]:
                    image["types"].append(mapped)
        cursor.close()
        return {pid: list(per_product.values()) for pid, per_product in images_by_product.items()}
    except Exception as e:
        log(
            f"Warning: Error fetching images for {len(product_ids)} products: {e}"
            " - will continue without images"
        )
        return {}


def get_prices_per_store(product_ids, conn):
    """Return nested map of product_id -> store_id -> price_info.

    NEW 2026-04-23 (pre-price-fix). Replaces the per-currency price join that
    used to live inside _PRODUCT_DETAIL_QUERY. pim.product_price is keyed by
    (product_id, store_id) -- not (product_id, currency_id) -- so we now fetch
    prices once, grouped by store_id, and the transform looks up a store's
    price directly when emitting its payload.

    price_info dict shape:
        {
          "retail_price": Decimal | None,
          "special_retail_price": Decimal | None,
          "cost_price": Decimal | None,
          "currency_code": str (ISO upper-case) | None,
        }

    If multiple rows exist for the same (product_id, store_id) across
    different currencies or valid_from windows, the row with the newest
    valid_from wins (NULLS LAST). store_id is assumed non-null per the
    data-model contract (confirmed 2026-04-23); rows with NULL store_id
    are excluded.
    """
    if not product_ids:
        return {}
    try:
        query = """
            SELECT DISTINCT ON (pp.product_id, pp.store_id)
                pp.product_id,
                pp.store_id,
                pp.retail_price,
                pp.special_retail_price,
                pp.cost_price,
                cur.code AS currency_code
            FROM pim.product_price pp
            LEFT JOIN pim.currency cur ON pp.currency_id = cur.currency_id
            WHERE pp.product_id = ANY(%s)
              AND pp.store_id IS NOT NULL
              AND (pp.valid_from IS NULL OR pp.valid_from <= NOW())
              AND (pp.valid_to   IS NULL OR pp.valid_to   >= NOW())
            ORDER BY pp.product_id, pp.store_id, pp.valid_from DESC NULLS LAST
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_ids,))
        prices = {}
        for product_id, store_id, retail, special, cost, currency_code in cursor.fetchall():
            prices.setdefault(product_id, {})[int(store_id)] = {
                "retail_price": retail,
                "special_retail_price": special,
                "cost_price": cost,
                "currency_code": currency_code.upper() if currency_code else None,
            }
        cursor.close()
        return prices
    except Exception as e:
        log(
            f"Warning: Error fetching per-store prices for {len(product_ids)} products: {e}"
            " - will continue, affected stores will be skipped with missing-price warnings"
        )
        return {}


def get_store_currency_map(conn):
    """Return map of store_code -> currency_code (ISO, upper-case).

    REMOVED from the main pipeline 2026-04-23 (pre-price-fix) -- each price
    row in pim.product_price now carries its own currency via currency_id,
    so the transform reads currency straight off the price row rather than
    doing a store->currency lookup. Kept here as a utility in case callers
    still want it, but get_product_details() no longer calls it.
    """
    try:
        query = """
            SELECT DISTINCT ON (s.store_id) s.code, c.code
            FROM pim.store s
            JOIN pim.store_currency sc ON s.store_id = sc.store_id
            JOIN pim.currency c ON sc.currency_id = c.currency_id
            ORDER BY s.store_id, sc.is_default DESC NULLS LAST
        """
        cursor = conn.cursor()
        cursor.execute(query)
        result = {
            store_code.lower(): currency_code.upper()
            for store_code, currency_code in cursor.fetchall()
            if store_code and currency_code
        }
        cursor.close()
        return result
    except Exception as e:
        log(
            f"Warning: Error fetching store currencies: {e}"
            " - will continue without store currency mapping"
        )
        return {}


def get_disabled_hardcoded_attribute_codes(conn):
    """Return hardcoded attribute codes that are disabled for sync.

    Reads pim.attribute_def.magento_sync and returns the lowered codes for
    any hardcoded attribute whose magento_sync flag is false.
    """
    hardcoded_codes = sorted(
        {
            str(code).lower()
            for attr_dict in (
                CLASSIC_HARDCODED_ATTRIBUTES,
                BNIB_SIMPLE_HARDCODED_ATTRIBUTES,
                BNIB_CONFIGURABLE_HARDCODED_ATTRIBUTES,
            )
            for code in attr_dict.keys()
        }
    )
    if not hardcoded_codes:
        return set()
    try:
        query = """
            SELECT LOWER(ad.code)
            FROM pim.attribute_def ad
            WHERE LOWER(ad.code) = ANY(%s)
              AND ad.magento_sync = false
        """
        cursor = conn.cursor()
        cursor.execute(query, (hardcoded_codes,))
        disabled = {row[0] for row in cursor.fetchall() if row and row[0]}
        cursor.close()
        return disabled
    except Exception as e:
        log(
            f"Warning: Error fetching disabled hardcoded attributes: {e}"
            " - will continue with defaults"
        )
        return set()


def _get_attribute_value(data_type, value_text, value_num, value_bool, value_json):
    data_type = (data_type or "").lower()
    if data_type in ("text", "string"):
        return value_text
    if data_type in ("num", "numeric", "number", "decimal"):
        return float(value_num) if value_num is not None else None
    if data_type in ("bool", "boolean"):
        return value_bool
    if data_type == "json":
        return value_json
    for value in (value_text, value_num, value_bool, value_json):
        if value is not None:
            return value
    return None


def get_product_custom_attributes(product_ids, conn):
    """Return custom (EAV) attributes keyed by product_id.

    Only global (store_id IS NULL) rows are put in the main product_id map --
    store-scoped overrides are keyed by (product_id, store_id) tuples. Each
    entry carries attribute_code, value, and magento_scope (defaulted to
    'global' if unset).
    """
    if not product_ids:
        return {}
    try:
        query = """
            SELECT pav.product_id, ad.code, ad.data_type, ad.magento_scope,
                   pav.value_text, pav.value_num, pav.value_bool, pav.value_json,
                   pav.store_id
            FROM pim.product_attribute_val pav
            JOIN pim.attribute_def ad ON pav.attribute_id = ad.attribute_id
            WHERE pav.product_id = ANY(%s)
              AND ad.magento_sync = true
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_ids,))
        attributes_map = {}
        for row in cursor.fetchall():
            (
                product_id,
                code,
                data_type,
                magento_scope,
                value_text,
                value_num,
                value_bool,
                value_json,
                store_id,
            ) = row
            value = _get_attribute_value(
                data_type, value_text, value_num, value_bool, value_json
            )
            if value is None or code is None:
                continue
            entry = {
                "attribute_code": code,
                "magento_scope": (magento_scope or "global"),
                "value": value,
            }
            key = product_id if store_id is None else (product_id, store_id)
            attributes_map.setdefault(key, []).append(entry)
        cursor.close()
        return attributes_map
    except Exception as e:
        log(
            f"Warning: Error fetching product custom attributes: {e}"
            " - will continue without them"
        )
        return {}


def get_product_name_attributes(product_ids, conn):
    """Return map of product_id -> best available name string.

    Picks the highest-priority of name / product_name / product_title per
    product, falling back as needed.
    """
    if not product_ids:
        return {}
    try:
        query = """
            SELECT pav.product_id, ad.code, ad.data_type,
                   pav.value_text, pav.value_num, pav.value_bool, pav.value_json
            FROM pim.product_attribute_val pav
            JOIN pim.attribute_def ad ON pav.attribute_id = ad.attribute_id
            WHERE pav.product_id = ANY(%s)
              AND lower(ad.code) IN ('name', 'product_name', 'product_title')
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_ids,))
        priority = {"name": 0, "product_name": 1, "product_title": 2}
        names_map = {}
        selected_priority = {}
        for row in cursor.fetchall():
            product_id, code, data_type, value_text, value_num, value_bool, value_json = row
            key = str(code).lower() if code else ""
            if key not in priority:
                continue
            value = _get_attribute_value(
                data_type, value_text, value_num, value_bool, value_json
            )
            if value is None:
                continue
            text_value = str(value).strip()
            if not text_value:
                continue
            rank = priority[key]
            if rank < selected_priority.get(product_id, 999):
                names_map[product_id] = text_value
                selected_priority[product_id] = rank
        cursor.close()
        return names_map
    except Exception as e:
        log(
            f"Warning: Error fetching product name attributes: {e}"
            " - will continue without them"
        )
        return {}


def get_team_season_for_products(product_ids, conn):
    """Return map of product_id -> {teams, seasons, departments} (comma-joined)."""
    if not product_ids:
        return {}
    try:
        query = """
            SELECT
                ptm.product_id,
                ARRAY_TO_STRING(ARRAY_AGG(DISTINCT c.name), ',') AS teams,
                ARRAY_TO_STRING(ARRAY_AGG(DISTINCT s.code), ',') AS seasons,
                ARRAY_TO_STRING(ARRAY_AGG(DISTINCT d.name), ',') AS departments
            FROM pim.product_team_meta ptm
            LEFT JOIN pim.club c ON ptm.club_id = c.club_id
            LEFT JOIN pim.season s ON ptm.season_id = s.season_id
            LEFT JOIN pim.department d ON ptm.department_id = d.department_id
            WHERE ptm.product_id = ANY(%s)
            GROUP BY ptm.product_id
        """
        cursor = conn.cursor()
        cursor.execute(query, (product_ids,))
        result = {}
        for product_id, teams, seasons, departments in cursor.fetchall():
            result[product_id] = {
                "teams": teams,
                "seasons": seasons,
                "departments": departments,
            }
        cursor.close()
        return result
    except Exception as e:
        log(
            f"Warning: Error fetching team/season data for {len(product_ids)} products: {e}"
            " - will continue without them"
        )
        return {}


# ── Core product fetch ───────────────────────────────────────────────────────

_PRODUCT_DETAIL_QUERY = """
    SELECT
        p.product_id, p.sku, p.parent_sku, p.product_type, p.name, p.product_description,
        p.country_of_manufacture, p.commodity_code, p.composition AS commodity_composition,
        p.product_style,
        b.brand_name, g.gender_name, pc.magento_colour_name,
        e.name AS era_name,
        p.barcode,
        p.weight, p.width_cm, p.length_cm, p.printed,
        ps.magento_size_short_name, ps.magento_size_long_name AS size_product,
        c.condition_name, tc.tax_class_name,
        pst.qty as stock_qty
    FROM pim.product p
    LEFT JOIN pim.brands b ON p.brand_id = b.brand_id
    LEFT JOIN pim.gender g ON p.gender_id = g.gender_id
    LEFT JOIN pim.product_colour pc ON p.product_colour_id = pc.product_colour_id
    LEFT JOIN pim.era e ON p.era_id = e.era_id
    LEFT JOIN pim.product_sizes ps ON p.size_id = ps.size_id
    LEFT JOIN pim.condition c ON p.condition_id = c.condition_id
    LEFT JOIN pim.tax_class tc ON p.tax_class_id = tc.tax_class_id
    LEFT JOIN pim.product_stock pst ON p.product_id = pst.product_id
    WHERE p.product_id = ANY(%s)
"""
# CHANGED 2026-04-23 (pre-price-fix backup): removed the product_price /
# currency joins from this query. Prices are now fetched per-store by
# get_prices_per_store() because the data model keys prices by
# (product_id, store_id) rather than (product_id, currency_id). Joining
# here would also cause row-multiplication for products with per-store
# overrides, which we'd then have to dedupe downstream.


def _expand_to_configurable_siblings(results, conn, cursor):
    """Fetch any siblings/parents missing from the initial result set.

    If we queued a configurable parent, pull all its children. If we queued
    a child, pull its parent + sibling children. This is the same expansion
    the v1/v2 code has always done; the only change vs pre-2026-04-23 is
    that it's now unconditional (previously it was also done for create
    mode only, but since we removed the create/update distinction, all
    queue items get the expansion).
    """
    fetched_ids = {row["product_id"] for row in results}
    parent_skus_to_expand = set()
    for result in results:
        product_type = (result.get("product_type") or "simple").lower()
        parent_sku = result.get("parent_sku")
        if product_type == "configurable":
            parent_skus_to_expand.add(result["sku"])
        elif parent_sku:
            parent_skus_to_expand.add(parent_sku)

    if not parent_skus_to_expand:
        return results

    expand_query = """
        SELECT product_id FROM pim.product
        WHERE (parent_sku = ANY(%s) OR sku = ANY(%s))
          AND product_id != ALL(%s)
    """
    cursor.execute(
        expand_query,
        (list(parent_skus_to_expand), list(parent_skus_to_expand), list(fetched_ids)),
    )
    sibling_ids = [row[0] for row in cursor.fetchall()]
    if not sibling_ids:
        return results

    log(
        f"Expanding configurable groups: fetching {len(sibling_ids)} additional "
        "sibling rows"
    )
    cursor.execute(_PRODUCT_DETAIL_QUERY, (sibling_ids,))
    columns = [desc[0] for desc in cursor.description]
    results.extend(dict(zip(columns, row)) for row in cursor.fetchall())
    return results


def get_product_details(product_ids):
    """Query pim.product and enrich with images, attributes, prices, etc.

    Returns a list of result dicts -- one per product row in the group. A
    queued configurable parent or child is expanded to include the full
    group so the whole configurable product is sent to Magento together.

    CHANGED 2026-04-23: removed the sync_mode_map parameter. Sibling expansion
    now happens unconditionally (same behaviour as create mode used to have).
    """
    if not product_ids:
        return []

    try:
        log(f"Fetching details for {len(product_ids)} products")
        global DISABLED_HARDCODED_ATTRIBUTE_CODES

        conn = _pg_connect()
        DISABLED_HARDCODED_ATTRIBUTE_CODES = get_disabled_hardcoded_attribute_codes(conn)
        if DISABLED_HARDCODED_ATTRIBUTE_CODES:
            log(
                "Skipping hardcoded attributes with magento_sync=false: "
                + ", ".join(sorted(DISABLED_HARDCODED_ATTRIBUTE_CODES))
            )

        cursor = conn.cursor()
        cursor.execute(_PRODUCT_DETAIL_QUERY, (product_ids,))
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        log(f"Fetched {len(results)} product records from pim.product")

        # Expand configurable groups: a queued parent pulls its children; a
        # queued child pulls its parent + sibling children.
        results = _expand_to_configurable_siblings(results, conn, cursor)

        all_product_ids = [r.get("product_id") for r in results if r.get("product_id")]

        # Map configurable_parent_sku -> its product_id. Used below so
        # child rows can borrow the parent's images / store assignments.
        parent_sku_to_product_id = {
            r["sku"]: r["product_id"]
            for r in results
            if (r.get("product_type") or "simple").lower() == "configurable"
        }

        # Which product_ids do we need images / store assignments for?
        # - Configurable parent: self.
        # - Simple child of a configurable: parent's id.
        # - Standalone simple: self.
        asset_product_ids = set()
        for r in results:
            product_type = (r.get("product_type") or "simple").lower()
            parent_sku = r.get("parent_sku")
            if product_type == "configurable":
                asset_product_ids.add(r["product_id"])
            elif parent_sku and parent_sku in parent_sku_to_product_id:
                asset_product_ids.add(parent_sku_to_product_id[parent_sku])
            else:
                asset_product_ids.add(r["product_id"])

        assignment_product_ids = set(all_product_ids) | asset_product_ids

        store_assignments_map = get_store_assignments_for_products(
            list(assignment_product_ids), conn
        )
        # NEW 2026-04-23 (pre-price-fix): prices fetched per-store, not joined
        # into the main product query. Keyed: {product_id: {store_id: info}}.
        # Each price info dict carries its own currency_code so we no longer
        # need a separate store->currency lookup.
        prices_by_store_map = get_prices_per_store(all_product_ids, conn)
        product_attributes_map = get_product_custom_attributes(all_product_ids, conn)
        product_names_map = get_product_name_attributes(all_product_ids, conn)
        images_map = get_images_for_products(list(asset_product_ids), conn)
        team_season_map = get_team_season_for_products(all_product_ids, conn)

        # Enrich each row
        for r in results:
            product_id = r["product_id"]
            product_type = (r.get("product_type") or "simple").lower()
            parent_sku = r.get("parent_sku")

            if product_type == "configurable":
                assignment_id = product_id
            elif parent_sku and parent_sku in parent_sku_to_product_id:
                assignment_id = parent_sku_to_product_id[parent_sku]
            else:
                assignment_id = product_id

            assignment = store_assignments_map.get(assignment_id, {})
            r["websites"] = assignment.get("website_codes", [])
            r["store_view_codes"] = assignment.get("store_view_codes", [])
            r["store_to_website_map"] = assignment.get("store_to_website_map", {})
            r["store_to_store_id_map"] = assignment.get("store_to_store_id_map", {})
            # Per-store prices for this product_id. Children have their own
            # row so each gets its own entry. Shape: {store_id: price_info}.
            r["prices_by_store_id"] = prices_by_store_map.get(product_id, {})
            r["product_custom_attributes"] = product_attributes_map.get(product_id, [])
            r["product_name_attribute"] = product_names_map.get(product_id)
            r["product_images"] = images_map.get(assignment_id, [])
            r["product_team_season"] = team_season_map.get(product_id)

        # Website-allowlist filter: drop any group whose parent has no allowed
        # website. Queue rows for skipped groups are left untouched so they
        # auto-retry once the website is added to ALLOWED_WEBSITE_CODES.
        if ALLOWED_WEBSITE_CODES:
            skipped_group_keys = set()
            for r in results:
                product_type = (r.get("product_type") or "simple").lower()
                group_key = (
                    r.get("sku")
                    if product_type == "configurable"
                    else (r.get("parent_sku") or r.get("sku"))
                )
                if not r.get("websites"):
                    skipped_group_keys.add(group_key)
            if skipped_group_keys:
                before = len(results)
                results = [
                    r
                    for r in results
                    if (
                        r.get("sku")
                        if (r.get("product_type") or "simple").lower() == "configurable"
                        else (r.get("parent_sku") or r.get("sku"))
                    )
                    not in skipped_group_keys
                ]
                log(
                    f"Skipped {before - len(results)} row(s) across "
                    f"{len(skipped_group_keys)} product group(s) - not assigned to "
                    f"any allowed website ({', '.join(ALLOWED_WEBSITE_CODES)}). "
                    "Queue entries left for retry."
                )

        cursor.close()
        conn.close()
        return results
    except Exception as e:
        log(f"Error fetching details for {len(product_ids)} products: {e}")
        raise


# =============================================================================
# SECTION 6: TRANSFORM HELPERS
# =============================================================================

def _merge_custom_attributes(primary, secondary):
    """Merge two custom_attributes lists, keeping the first value seen per code."""
    if not secondary:
        return primary
    existing = {item.get("attribute_code") for item in primary if isinstance(item, dict)}
    merged = list(primary)
    for item in secondary:
        code = item.get("attribute_code") if isinstance(item, dict) else None
        if code in existing:
            continue
        merged.append(item)
        existing.add(code)
    return merged


def _upsert_custom_attribute(custom_attributes, attribute_code, value, magento_scope=None):
    """Add or update one attribute entry, returning a new list."""
    code_lower = str(attribute_code or "").lower().strip()
    if not code_lower:
        return [dict(a) for a in (custom_attributes or []) if isinstance(a, dict)]

    merged = []
    existing = None
    for a in custom_attributes or []:
        if not isinstance(a, dict):
            continue
        copied = dict(a)
        merged.append(copied)
        if str(copied.get("attribute_code") or "").lower().strip() == code_lower:
            existing = copied

    if value is None:
        return merged
    text_value = str(value).strip()
    if not text_value:
        return merged

    if existing is not None:
        existing["value"] = text_value
        if magento_scope and not existing.get("magento_scope"):
            existing["magento_scope"] = magento_scope
        return merged

    new_entry = {"attribute_code": attribute_code, "value": text_value}
    if magento_scope:
        new_entry["magento_scope"] = magento_scope
    merged.append(new_entry)
    return merged


def _apply_hardcoded_custom_attributes(custom_attributes, hardcoded_dict):
    """Apply hardcoded attribute values to a custom_attributes list.

    hardcoded_dict should be one of CLASSIC_HARDCODED_ATTRIBUTES,
    BNIB_SIMPLE_HARDCODED_ATTRIBUTES, or BNIB_CONFIGURABLE_HARDCODED_ATTRIBUTES.
    """
    merged = []
    existing_by_code = {}
    for a in custom_attributes or []:
        if not isinstance(a, dict):
            continue
        code = a.get("attribute_code")
        if not code:
            continue
        copied = dict(a)
        merged.append(copied)
        existing_by_code.setdefault(str(code).lower(), copied)

    for code, value in hardcoded_dict.items():
        code_lower = str(code).lower()
        if code_lower in DISABLED_HARDCODED_ATTRIBUTE_CODES:
            continue
        existing = existing_by_code.get(code_lower)
        if existing is not None:
            existing["value"] = value
            continue
        merged.append({"attribute_code": code, "value": value})
    return merged


def _get_custom_attribute_value(custom_attributes, attribute_codes):
    if not custom_attributes:
        return None
    wanted = {code.lower() for code in attribute_codes}
    for a in custom_attributes:
        if not isinstance(a, dict):
            continue
        code = a.get("attribute_code")
        if not code or str(code).lower() not in wanted:
            continue
        value = a.get("value")
        if value is None:
            continue
        text_value = str(value).strip()
        if text_value:
            return text_value
    return None


def _is_bnib_product(custom_attributes):
    """Is this a BNIB product? Based on the bnib EAV attribute, not structure."""
    value = _get_custom_attribute_value(custom_attributes, ["bnib"])
    return value is not None and value.lower() == "yes"


def _get_hardcoded_attributes(custom_attributes, is_configurable):
    """Return the right hardcoded attribute dict for a product."""
    if _is_bnib_product(custom_attributes):
        if is_configurable:
            return BNIB_CONFIGURABLE_HARDCODED_ATTRIBUTES
        return BNIB_SIMPLE_HARDCODED_ATTRIBUTES
    return CLASSIC_HARDCODED_ATTRIBUTES


def _resolve_product_name(first, parent_sku):
    """Pick a name from the PIM row, falling back to the SKU."""
    for field in ("name", "product_description"):
        value = first.get(field)
        if value is not None:
            text_value = str(value).strip()
            if text_value:
                return text_value
    return parent_sku


def _format_name_with_size(product_name, size_value):
    base = str(product_name).strip() if product_name is not None else ""
    size = str(size_value).strip() if size_value is not None else ""
    if base and size:
        return f"{base} - {size}"
    return base or size


def _filter_attributes_for_store_views(custom_attributes, store_codes):
    """Drop attributes that don't belong in a specific store-view payload.

    - Attributes listed in STORE_VIEW_EXCLUDED_ATTRIBUTES[store_code] are removed.
    - Attributes ending in "_us" are only sent to the US store view.
    """
    if not custom_attributes:
        return custom_attributes

    excluded = set()
    if store_codes and STORE_VIEW_EXCLUDED_ATTRIBUTES:
        for sc in store_codes:
            excluded.update(STORE_VIEW_EXCLUDED_ATTRIBUTES.get(sc, set()))

    is_us_store = bool(store_codes) and any(sc.lower() == "us" for sc in store_codes)

    def keep(attr):
        if not isinstance(attr, dict):
            return True
        code = attr.get("attribute_code") or ""
        if code in excluded:
            return False
        if code.endswith("_us") and not is_us_store:
            return False
        return True

    return [a for a in custom_attributes if keep(a)]


def _build_parent_core_custom_attributes(first, team_season):
    """Build custom attributes from core product table fields + team/season."""
    mappings = [
        ("brand_n", first.get("brand_name")),
        ("colour", first.get("magento_colour_name")),
        ("countryofmanufacturer", first.get("country_of_manufacture")),
        ("commodity_code", first.get("commodity_code")),
        ("commodity_composition", first.get("commodity_composition")),
        ("product_style", first.get("product_style")),
        ("era", first.get("era_name")),
        ("teams", team_season.get("teams") if team_season else None),
        ("seasons", team_season.get("seasons") if team_season else None),
        ("department", team_season.get("departments") if team_season else None),
    ]
    out = []
    for code, value in mappings:
        if value is not None and str(value).strip():
            out.append(
                {
                    "attribute_code": code,
                    "value": str(value).strip(),
                    "magento_scope": "global",
                }
            )
    return out


def _build_variant_core_custom_attributes(v):
    """Build custom attributes from per-variant fields (size / dims / barcode)."""
    mappings = [
        ("size_product", v.get("magento_size_short_name")),
        ("description_size_guide_width_cm", v.get("width_cm")),
        ("description_size_guide_length_cm", v.get("length_cm")),
        (
            "printed",
            (1 if v.get("printed") else 0) if v.get("printed") is not None else None,
        ),
        ("barcode_new", v.get("barcode")),
    ]
    out = []
    for code, value in mappings:
        if value is not None and str(value).strip():
            out.append(
                {
                    "attribute_code": code,
                    "value": str(value).strip(),
                    "magento_scope": "global",
                }
            )
    return out


def _filter_skipped_attributes(custom_attributes):
    """Remove SKIPPED_ATTRIBUTE_CODES from a custom_attributes list."""
    if not custom_attributes or not SKIPPED_ATTRIBUTE_CODES:
        return custom_attributes
    return [
        a
        for a in custom_attributes
        if not isinstance(a, dict) or a.get("attribute_code") not in SKIPPED_ATTRIBUTE_CODES
    ]


# =============================================================================
# SECTION 7: PAYLOAD BUILDER
# =============================================================================

def _build_scoped_product(
    first,
    group_rows,
    parent_sku,
    parent_payload_custom_attributes,
    product_name,
    product_images,
    websites,
    store_to_website,
    currency_code,
    store_codes,
    prices_for_store,
    store_id=None,
):
    """Build one scoped payload dict for a single store (or unscoped).

    CHANGED 2026-04-23 (pre-price-fix): used to take a per-currency
    `variant_prices_map` keyed by currency_code. Now takes `prices_for_store`,
    a flat {product_id: price_info} dict already resolved for the single
    target store, plus the store_id it came from (for logging).

    Returns None if the price for this store is missing for the parent (simple)
    or for any child (configurable) -- the caller logs and skips the store.

    The returned dict contains store_view_codes, websites, and the website-
    scoped price fields (price, special_price, cost) all on the same entry.
    Magento routes each to its correct scope on its own.
    """
    product_type = (first.get("product_type") or "simple").lower()
    is_configurable = product_type == "configurable"
    store_code_for_log = store_codes[0] if store_codes else "(no-store)"

    # Collect ALL product_ids in this group so their queue entries can all
    # be cleared when Alumio accepts the batch.
    all_group_product_ids = list(
        {row.get("product_id") for row in group_rows if row.get("product_id")}
    )

    scoped_product = {
        "_product_id": first.get("product_id"),
        "_all_product_ids": all_group_product_ids,
        "sku": parent_sku,
        "attribute_set_code": "Default",
        "product_type": "configurable" if is_configurable else "simple",
        "name": product_name,
        "description": first.get("product_description") or "",
        "url_key": parent_sku.lower().replace("_", "-").replace(".", "-"),
        "websites": list(websites) if websites else [],
        "_store_to_website": store_to_website,
        "status": 1,
        "visibility": 4,
    }
    if is_configurable:
        scoped_product["children"] = []
    if product_images:
        scoped_product["images"] = product_images
    if store_codes:
        scoped_product["store_view_codes"] = list(store_codes)
    if parent_payload_custom_attributes:
        scoped_product["custom_attributes"] = _filter_attributes_for_store_views(
            parent_payload_custom_attributes, store_codes
        )

    if is_configurable:
        scoped_product["configurable_attributes"] = ["size_config"]

        # Dedupe children by product_id -- historically needed because the
        # SQL joined product_price (one row per currency) and product_stock
        # (one row per warehouse). Prices are now fetched separately so
        # post-2026-04-23 the duplication only comes from product_stock,
        # but the dedupe remains cheap insurance.
        seen_child_product_ids = set()
        child_rows = []
        for row in group_rows:
            if (row.get("product_type") or "simple").lower() == "configurable":
                continue
            pid = row.get("product_id")
            if pid in seen_child_product_ids:
                continue
            seen_child_product_ids.add(pid)
            child_rows.append(row)

        for row in child_rows:
            size_value = row.get("magento_size_short_name") or ""
            # Per-store price lookup (CHANGED 2026-04-23 from per-currency).
            price_info = prices_for_store.get(row.get("product_id")) if store_codes else None
            if store_codes and not price_info:
                log(
                    f"Warning: Missing price for store '{store_code_for_log}' "
                    f"(store_id={store_id}) on child {row.get('sku')} of {parent_sku}"
                    " - skipping this store for the whole configurable"
                )
                return None

            child = {
                "sku": row.get("sku", ""),
                "name": _format_name_with_size(product_name, size_value),
                "status": 1,
                "visibility": 1,  # Not Visible Individually
                "price": _to_float_or(price_info, "retail_price", default=0),
                "special_price": _to_float_or(price_info, "special_retail_price", default=None),
                "cost": _to_float_or(price_info, "cost_price", default=0),
                "url_key": row.get("sku", "").lower().replace("_", "-").replace(".", "-"),
                "weight": float(row["weight"]) if row.get("weight") is not None else None,
                "stock": {
                    "qty": int(row["stock_qty"]) if row.get("stock_qty") else 0,
                    "is_in_stock": bool(row.get("stock_qty")),
                },
                "custom_attributes": [
                    {
                        "value": size_value,
                        "attribute_code": "size_config",
                        "magento_scope": "global",
                    }
                ],
            }
            tax_class_name = row.get("tax_class_name")
            if tax_class_name:
                child["tax_class_name"] = tax_class_name

            child["custom_attributes"] = _merge_custom_attributes(
                child["custom_attributes"],
                _build_variant_core_custom_attributes(row),
            )
            child["custom_attributes"] = _merge_custom_attributes(
                child["custom_attributes"],
                _filter_attributes_for_store_views(
                    parent_payload_custom_attributes, store_codes
                ),
            )
            child["custom_attributes"] = _merge_custom_attributes(
                child["custom_attributes"],
                row.get("product_custom_attributes", []),
            )
            child["custom_attributes"] = _apply_hardcoded_custom_attributes(
                child.get("custom_attributes", []),
                _get_hardcoded_attributes(
                    child.get("custom_attributes", []), is_configurable=False
                ),
            )
            child["custom_attributes"] = _filter_attributes_for_store_views(
                child["custom_attributes"], store_codes
            )
            if store_codes:
                child["store_view_codes"] = list(store_codes)
            scoped_product["children"].append(child)

        # Inherit tax_class_name from first child that has one
        if "tax_class_name" not in scoped_product:
            for child in scoped_product.get("children", []):
                if child.get("tax_class_name"):
                    scoped_product["tax_class_name"] = child["tax_class_name"]
                    break

        scoped_product["custom_attributes"] = _apply_hardcoded_custom_attributes(
            scoped_product.get("custom_attributes", []),
            _get_hardcoded_attributes(
                scoped_product.get("custom_attributes", []), is_configurable=True
            ),
        )
        scoped_product["custom_attributes"] = _filter_attributes_for_store_views(
            scoped_product["custom_attributes"], store_codes
        )
    else:
        # Simple product (Classic or standalone BNIB)
        row = group_rows[0]
        # Per-store price lookup (CHANGED 2026-04-23 from per-currency).
        price_info = prices_for_store.get(row.get("product_id")) if store_codes else None
        if store_codes and not price_info:
            log(
                f"Warning: Missing price for store '{store_code_for_log}' "
                f"(store_id={store_id}) on product {parent_sku}"
                " - skipping this store"
            )
            return None

        scoped_product["price"] = _to_float_or(price_info, "retail_price", default=0)
        scoped_product["special_price"] = _to_float_or(
            price_info, "special_retail_price", default=None
        )
        scoped_product["cost"] = _to_float_or(price_info, "cost_price", default=0)
        scoped_product["weight"] = (
            float(row["weight"]) if row.get("weight") is not None else None
        )
        scoped_product["stock"] = {
            "qty": int(row["stock_qty"]) if row.get("stock_qty") else 0,
            "is_in_stock": bool(row.get("stock_qty")),
        }
        tax_class_name = row.get("tax_class_name")
        if tax_class_name:
            scoped_product["tax_class_name"] = tax_class_name

        scoped_product["custom_attributes"] = _merge_custom_attributes(
            scoped_product.get("custom_attributes", []),
            _build_variant_core_custom_attributes(row),
        )
        scoped_product["custom_attributes"] = _merge_custom_attributes(
            scoped_product.get("custom_attributes", []),
            row.get("product_custom_attributes", []),
        )
        scoped_product["custom_attributes"] = _apply_hardcoded_custom_attributes(
            scoped_product.get("custom_attributes", []),
            _get_hardcoded_attributes(
                scoped_product.get("custom_attributes", []), is_configurable=False
            ),
        )
        if not _is_bnib_product(scoped_product.get("custom_attributes", [])):
            scoped_product["description"] = "<p>Classic Football Shirts</p>"
        scoped_product["custom_attributes"] = _filter_attributes_for_store_views(
            scoped_product["custom_attributes"], store_codes
        )

    return scoped_product


def _to_float_or(price_info, key, default):
    """Helper: read a decimal-ish value from price_info, coerce to float."""
    if price_info and price_info.get(key):
        return float(price_info[key])
    return default


def transform_to_magento(products):
    """Turn PIM product rows into a flat list of Magento payload entries.

    CHANGED 2026-04-23: iterates per-store (not per-currency). Prices live in
    pim.product_price keyed by (product_id, store_id), so we emit one scoped
    payload per store-view and look up that store's own price row. Each entry
    carries store_view_codes, websites, and the website-scoped price fields
    together -- Magento/Alumio route each field to its correct scope.

    Previously iterated per-currency, grouping stores that share a currency.
    That worked when pricing was one-row-per-currency; it no longer does.
    """
    # Group rows by parent_sku (configurable child) or sku (standalone simple).
    grouped = {}
    for p in products:
        group_key = p.get("parent_sku") or p.get("sku")
        grouped.setdefault(group_key, []).append(p)

    magento_products = []

    for parent_sku, group_rows in grouped.items():
        # Configurable parent row (if present) must be first so we read the
        # parent's custom_attributes etc. from it.
        group_rows.sort(
            key=lambda r: 0
            if (r.get("product_type") or "simple").lower() == "configurable"
            else 1
        )
        first = group_rows[0]
        parent_custom_attributes = first.get("product_custom_attributes", [])
        team_season = first.get("product_team_season")

        # Merge in the things we derive from pim.product and team/season data.
        parent_payload_custom_attributes = _upsert_custom_attribute(
            parent_custom_attributes,
            "gender",
            first.get("gender_name"),
            magento_scope="global",
        )
        parent_payload_custom_attributes = _merge_custom_attributes(
            parent_payload_custom_attributes,
            _build_parent_core_custom_attributes(first, team_season),
        )

        product_name = _resolve_product_name(first, parent_sku)
        product_images = first.get("product_images", [])
        websites = first.get("websites", [])
        store_view_codes = first.get("store_view_codes", [])
        store_to_website = first.get("store_to_website_map", {})
        store_to_store_id = first.get("store_to_store_id_map", {})

        # Build {product_id: {store_id: price_info}} by collating the
        # prices_by_store_id on each row in the group. Same info as
        # prices_by_store_map in get_product_details, scoped to this group.
        group_prices_by_product = {}
        for row in group_rows:
            pid = row.get("product_id")
            if pid is None:
                continue
            group_prices_by_product.setdefault(pid, {}).update(
                row.get("prices_by_store_id") or {}
            )

        # If the product has no store assignments, emit a single unscoped
        # payload. The [None] sentinel keeps the emission loop simple.
        store_codes_to_emit = list(store_view_codes) if store_view_codes else [None]

        for store_code in store_codes_to_emit:
            store_id = store_to_store_id.get(store_code) if store_code else None

            # Build {product_id: price_info} for just this store.
            prices_for_store = {}
            if store_id is not None:
                for pid, by_store in group_prices_by_product.items():
                    info = by_store.get(store_id)
                    if info is not None:
                        prices_for_store[pid] = info

            # Resolve the currency for logs/debug. If the row for `first`
            # has a price at this store, use its currency_code. Otherwise
            # fall back to None -- the missing-price branch downstream
            # will already skip this store.
            currency_code = None
            first_price = prices_for_store.get(first.get("product_id"))
            if first_price:
                currency_code = first_price.get("currency_code")

            scoped = _build_scoped_product(
                first=first,
                group_rows=group_rows,
                parent_sku=parent_sku,
                parent_payload_custom_attributes=parent_payload_custom_attributes,
                product_name=product_name,
                product_images=product_images,
                websites=websites,
                store_to_website=store_to_website,
                currency_code=currency_code,
                store_codes=[store_code] if store_code else [],
                prices_for_store=prices_for_store,
                store_id=store_id,
            )
            if scoped is None:
                continue
            magento_products.append(scoped)

    return magento_products


# =============================================================================
# SECTION 8: OUTPUT & SEND
# =============================================================================

def _strip_internal_keys(product):
    """Recursively remove leading-underscore and scope keys from a payload."""

    def clean(value, is_not_visible=False):
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, dict):
            not_visible = is_not_visible or value.get("visibility") == 1
            cleaned = {}
            for key, item in value.items():
                if key in (
                    "_product_id",
                    "_all_product_ids",
                    "_store_to_website",
                    "_sync_mode",
                    "magento_scope",
                ):
                    continue
                if key == "custom_attributes" and isinstance(item, list):
                    item = _filter_skipped_attributes(item)
                    if not_visible and _VISIBLE_ONLY_ATTRIBUTE_CODES:
                        item = [
                            a
                            for a in item
                            if not isinstance(a, dict)
                            or a.get("attribute_code") not in _VISIBLE_ONLY_ATTRIBUTE_CODES
                        ]
                if key == "children" and isinstance(item, list):
                    cleaned[key] = [clean(child, is_not_visible=False) for child in item]
                    continue
                cleaned[key] = clean(item, is_not_visible=not_visible)
            return cleaned
        return value

    return clean(product)


def _to_json_compatible(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(item) for item in value]
    return value


def _log_alumio_payload(payload_json):
    try:
        with open(ALUMIO_PAYLOAD_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(payload_json + "\n")
    except Exception as e:
        log(f"WARNING: Failed to write Alumio payload log: {e}")


def _batch_products(products):
    """Chunk products into batches keeping entries for the same product_id together."""
    groups = []
    group_index = {}

    for product in products:
        product_id = product.get("_product_id")
        if product_id in group_index:
            groups[group_index[product_id]].append(product)
        else:
            group_index[product_id] = len(groups)
            groups.append([product])

    batches = []
    current_batch = []
    current_product_ids = 0
    for group in groups:
        if current_batch and current_product_ids >= MAX_PRODUCT_IDS_PER_BATCH:
            batches.append(current_batch)
            current_batch = []
            current_product_ids = 0
        current_batch.extend(group)
        current_product_ids += 1

    if current_batch:
        batches.append(current_batch)
    return batches


def send_to_alumio(products):
    """POST products to Alumio in batches. Return (success, fail, succ_ids, fail_ids)."""
    batches = _batch_products(products)

    success_count = 0
    fail_count = 0
    failed_skus = []
    success_product_ids = set()
    failed_product_ids = set()

    log(f"Sending {len(batches)} batches ({len(products)} products) to Alumio")

    for i, batch in enumerate(batches, 1):
        batch_product_ids = set()
        for product in batch:
            # _all_product_ids (parent + children) so the full group's queue
            # entries are cleared when a group is sent successfully.
            all_ids = product.get("_all_product_ids")
            if all_ids:
                batch_product_ids.update(all_ids)
            elif product.get("_product_id"):
                batch_product_ids.add(product["_product_id"])

        try:
            payload = {
                "products": [
                    _to_json_compatible(_strip_internal_keys(p)) for p in batch
                ]
            }
            payload_json = json.dumps(payload)
            _log_alumio_payload(payload_json)

            response = requests.post(
                ALUMIO_WEBHOOK_URL,
                data=payload_json,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code in (200, 201, 202):
                success_count += len(batch)
                success_product_ids.update(batch_product_ids)
                log(f"Batch {i}/{len(batches)} OK ({len(batch)} products)")
            else:
                fail_count += len(batch)
                failed_product_ids.update(batch_product_ids)
                failed_skus.extend([p.get("sku") for p in batch])
                log(f"Batch {i}/{len(batches)} FAIL status={response.status_code}")
                log(f"Response: {response.text[:500]}")

            time.sleep(1)
        except Exception as e:
            fail_count += len(batch)
            failed_product_ids.update(batch_product_ids)
            failed_skus.extend([p.get("sku") for p in batch])
            log(f"Batch {i}/{len(batches)} ERROR: {str(e)[:300]}")

    if failed_skus:
        preview = ", ".join(filter(None, failed_skus[:20]))
        log(f"Failed SKUs: {preview}{'...' if len(failed_skus) > 20 else ''}")

    success_product_ids -= failed_product_ids
    return success_count, fail_count, success_product_ids, failed_product_ids


# =============================================================================
# SECTION 9: MAIN
# =============================================================================

log("=== PIM to Magento Sync Started (v2 - Flattened Schema) ===")
log(f"Alumio payload logging enabled: {ALUMIO_PAYLOAD_LOG_PATH}")

# Truncate the payload log at the start of every run so it only ever contains
# the payloads from the current run (relied on by recover_sync_queue.py).
try:
    open(ALUMIO_PAYLOAD_LOG_PATH, "w", encoding="utf-8").close()
except Exception as e:
    log(f"WARNING: Failed to truncate Alumio payload log: {e}")

try:
    last_sync = get_last_sync_time()
    log(f"Last sync: {last_sync}")

    product_ids = get_updated_products(last_sync)
    log(f"Found {len(product_ids)} updated products")

    if not product_ids:
        log("No products to sync")
    else:
        products = get_product_details(product_ids)
        log(f"Fetched {len(products)} product records")

        if not products:
            log("WARNING: Found product IDs but no product details returned - skipping sync")
        else:
            magento_products = transform_to_magento(products)
            order_index = {pid: i for i, pid in enumerate(product_ids)}
            magento_products.sort(
                key=lambda p: order_index.get(p.get("_product_id"), len(order_index))
            )
            log(f"Transformed to {len(magento_products)} Magento payload entries")

            success, failed, success_ids, failed_ids = send_to_alumio(magento_products)
            log(f"Alumio response: {success} success, {failed} failed")

            update_alumio_sync_queue(success_ids, failed_ids)

            if failed > 0:
                raise Exception(
                    f"SYNC FAILED: {failed}/{len(magento_products)} products failed "
                    "to send to Alumio - will retry failed products next run"
                )

            update_sync_status(len(magento_products), success)
            log(f"Summary: {success} products synced successfully")

    log("=== Sync Completed Successfully ===")

except Exception as e:
    log(f"FATAL ERROR: Sync failed - {e}")
    raise

finally:
    # Housekeeping: prune old 'success' rows so the queue stays bounded.
    # Runs whether the sync succeeded or failed; the function itself is
    # best-effort and won't raise.
    cleanup_old_success_rows(retention_days=30)
