import os
import json
import psycopg2
import requests
import time
from datetime import datetime

DB_HOST = os.environ.get("PIM_POSTGRES_HOST")
DB_NAME = os.environ.get("PIM_POSTGRES_NAME")
DB_USER = os.environ.get("PIM_POSTGRES_USER")
DB_PASSWORD = os.environ.get("PIM_POSTGRES_PASSWORD")
ALUMIO_WEBHOOK_URL = os.environ.get("ALUMIO_PRODUCT_UPSERT_WEBHOOK_URL")
MAX_PRODUCT_IDS_PER_BATCH = int(os.environ.get("ALUMIO_BATCH_SIZE", "10"))
LOG_ALUMIO_PAYLOADS = os.environ.get("LOG_ALUMIO_PAYLOADS", "").lower() in ("1", "true", "yes")
ALUMIO_PAYLOAD_LOG_PATH = os.environ.get(
    "ALUMIO_CATEGORY_PAYLOAD_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "alumio_category_payloads.jsonl"),
)
CATEGORY_OPERATION_SQL = "LOWER(REPLACE(COALESCE(operation_type, ''), ' ', '_')) = 'category_change'"

required_vars = {
    "PIM_POSTGRES_HOST": DB_HOST,
    "PIM_POSTGRES_NAME": DB_NAME,
    "PIM_POSTGRES_USER": DB_USER,
    "PIM_POSTGRES_PASSWORD": DB_PASSWORD,
    "ALUMIO_PRODUCT_UPSERT_WEBHOOK_URL": ALUMIO_WEBHOOK_URL,
}
missing = [k for k, v in required_vars.items() if not v]
if missing:
    raise Exception(f"Missing required environment variables: {', '.join(missing)}")


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def get_category_operation_product_ids():
    try:
        log(f"Connecting to PIM database: {DB_HOST}:{DB_NAME}")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=5432,
        )
        query = f"""
            SELECT product_id, MIN(created_at) AS first_created
            FROM pim.alumio_sync_queue
            WHERE status IN ('pending', 'failed')
              AND ({CATEGORY_OPERATION_SQL})
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
        log(f"Error querying category operations from Alumio sync queue: {e}")
        raise


def get_category_payload_products(product_ids):
    if not product_ids:
        return [], set()

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=5432,
        )
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT product_id, product_sku
            FROM pim.product
            WHERE product_id = ANY(%s)
            """,
            (product_ids,),
        )
        sku_map = {product_id: product_sku for product_id, product_sku in cursor.fetchall() if product_sku}

        cursor.execute(
            """
            WITH RECURSIVE category_hierarchy AS (
                SELECT pc.product_id, c.category_id, c.path_key, 0 AS level
                FROM pim.product_category pc
                JOIN pim.category c ON pc.category_id = c.category_id
                WHERE pc.product_id = ANY(%s)

                UNION ALL

                SELECT ch.product_id, c.category_id, c.path_key, ch.level + 1
                FROM category_hierarchy ch
                JOIN pim.category c ON ch.category_id = c.parent_id
                WHERE c.parent_id IS NOT NULL AND ch.level < 10
            )
            SELECT DISTINCT product_id, path_key
            FROM category_hierarchy
            ORDER BY product_id, path_key
            """,
            (product_ids,),
        )

        categories_map = {product_id: [] for product_id in product_ids}
        for product_id, path_key in cursor.fetchall():
            if path_key:
                categories_map.setdefault(product_id, []).append(path_key)

        cursor.close()
        conn.close()

        payload_products = []
        missing_product_ids = set()
        for product_id in product_ids:
            sku = sku_map.get(product_id)
            if not sku:
                missing_product_ids.add(product_id)
                log(f"Warning: Missing SKU for product_id {product_id}; marking as failed")
                continue

            payload_products.append(
                {
                    "_product_id": product_id,
                    "sku": sku,
                    "categories": categories_map.get(product_id, []),
                }
            )

        return payload_products, missing_product_ids
    except Exception as e:
        log(f"Error building category payload products: {e}")
        raise


def _batch_products(products):
    batches = []
    for i in range(0, len(products), MAX_PRODUCT_IDS_PER_BATCH):
        batches.append(products[i : i + MAX_PRODUCT_IDS_PER_BATCH])
    return batches


def _strip_internal_keys(product):
    return {k: v for k, v in product.items() if k != "_product_id"}


def send_to_alumio(products):
    if not products:
        return 0, 0, set(), set()

    batches = _batch_products(products)
    success_count = 0
    fail_count = 0
    failed_skus = []
    success_product_ids = set()
    failed_product_ids = set()

    log(f"Sending {len(batches)} batches ({len(products)} category products) to Alumio")

    for i, batch in enumerate(batches, 1):
        batch_product_ids = {product.get("_product_id") for product in batch}
        payload = {"products": [_strip_internal_keys(product) for product in batch]}

        try:
            if LOG_ALUMIO_PAYLOADS:
                with open(ALUMIO_PAYLOAD_LOG_PATH, "a") as payload_log:
                    payload_log.write(json.dumps(payload) + "\n")

            response = requests.post(
                ALUMIO_WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code in [200, 201, 202]:
                success_count += len(batch)
                success_product_ids.update(batch_product_ids)
                log(f"Batch {i}/{len(batches)} ✓ ({len(batch)} products)")
            else:
                fail_count += len(batch)
                failed_product_ids.update(batch_product_ids)
                failed_skus.extend([p["sku"] for p in batch])
                log(f"Batch {i}/{len(batches)} ✗ Status: {response.status_code}")
                log(f"Response: {response.text[:500]}")

            time.sleep(1)
        except Exception as e:
            fail_count += len(batch)
            failed_product_ids.update(batch_product_ids)
            failed_skus.extend([p["sku"] for p in batch])
            log(f"Batch {i}/{len(batches)} ERROR: {str(e)[:300]}")

    if failed_skus:
        log(f"Failed SKUs: {', '.join(failed_skus[:20])}{'...' if len(failed_skus) > 20 else ''}")

    success_product_ids -= failed_product_ids
    return success_count, fail_count, success_product_ids, failed_product_ids


def update_category_sync_queue(success_product_ids, failed_product_ids):
    try:
        if not success_product_ids and not failed_product_ids:
            return

        log("Updating category operations in Alumio sync queue")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=5432,
        )
        cursor = conn.cursor()
        deleted = 0
        failed = 0

        if success_product_ids:
            cursor.execute(
                f"""
                DELETE FROM pim.alumio_sync_queue
                WHERE status IN ('pending', 'failed')
                  AND product_id = ANY(%s)
                  AND ({CATEGORY_OPERATION_SQL})
                """,
                (list(success_product_ids),),
            )
            deleted = cursor.rowcount

        if failed_product_ids:
            cursor.execute(
                f"""
                UPDATE pim.alumio_sync_queue
                SET status = 'failed', processed_at = NOW()
                WHERE status IN ('pending', 'failed')
                  AND product_id = ANY(%s)
                  AND ({CATEGORY_OPERATION_SQL})
                """,
                (list(failed_product_ids),),
            )
            failed = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()

        if deleted:
            log(f"Cleared {deleted} queued category change(s)")
        if failed:
            log(f"Marked {failed} queued category change(s) as failed")
    except Exception as e:
        log(f"Error updating category operations in Alumio sync queue: {e}")
        raise


log("=== PIM Category to Magento Sync Started ===")

try:
    product_ids = get_category_operation_product_ids()
    log(f"Found {len(product_ids)} queued category products")

    if not product_ids:
        log("No category products to sync")
        log("=== Category Sync Completed Successfully ===")
    else:
        payload_products, missing_product_ids = get_category_payload_products(product_ids)
        log(f"Built category payload for {len(payload_products)} products")

        success = 0
        failed = 0
        success_product_ids = set()
        failed_product_ids = set(missing_product_ids)

        if payload_products:
            success, failed, success_product_ids, send_failed_product_ids = send_to_alumio(payload_products)
            failed_product_ids.update(send_failed_product_ids)

        update_category_sync_queue(success_product_ids, failed_product_ids)

        total_failed = failed + len(missing_product_ids)
        log(f"Alumio response: {success} success, {total_failed} failed")

        if total_failed > 0:
            raise Exception(
                f"SYNC FAILED: {total_failed}/{len(product_ids)} category products failed to send to Alumio "
                "- will retry failed products next run"
            )

        log(f"Summary: {success} category products synced successfully")
        log("=== Category Sync Completed Successfully ===")

except Exception as e:
    log(f"FATAL ERROR: Category sync failed - {e}")
    raise
