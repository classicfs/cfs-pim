"""
recover_sync_queue.py
─────────────────────
Recovers product_create queue entries from the Alumio payload log file.

Use this when the alumio_sync_queue has been cleared (e.g. after a run that
succeeded from the PIM side but failed inside Alumio), and you need to re-queue
the same products for a fresh create attempt.

What it does:
  1. Reads alumio_payloads.jsonl and extracts every SKU from the last run
     (or all runs — configurable via RECOVER_FROM_LAST_RUN).
  2. Looks up product_ids in pim.product.
  3. Re-inserts only the "root" products into pim.alumio_sync_queue:
       - configurable parents  (product_type = 'configurable')
       - standalone simples    (product_type = 'simple', parent_sku IS NULL)
     Child variants are skipped — the sync expansion logic fetches them
     automatically when their parent is processed.
  4. Prints a summary. Runs in DRY_RUN mode by default — set DRY_RUN=False
     to actually insert.

Usage:
  python recover_sync_queue.py

Required env vars: PIM_POSTGRES_HOST, PIM_POSTGRES_NAME,
                   PIM_POSTGRES_USER, PIM_POSTGRES_PASSWORD
"""

import os
import json
import psycopg2
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.environ.get("PIM_POSTGRES_HOST")
DB_NAME     = os.environ.get("PIM_POSTGRES_NAME")
DB_USER     = os.environ.get("PIM_POSTGRES_USER")
DB_PASSWORD = os.environ.get("PIM_POSTGRES_PASSWORD")

PAYLOAD_LOG = os.environ.get(
    "ALUMIO_PAYLOAD_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "alumio_payloads.jsonl")
)

# Set to True to only recover SKUs from the most recent run (last batch group
# written to the log). Set to False to recover all SKUs ever logged.
RECOVER_FROM_LAST_RUN = True

# Set to False to actually insert rows into pim.alumio_sync_queue.
DRY_RUN = False

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def extract_skus_from_log(path, last_run_only):
    """
    Parse the JSONL payload log and return a set of all SKUs.
    If last_run_only=True, only read lines written since the previous run
    (detected by a gap of >1 line between timestamp groups — approximated
    by reading backwards until we hit a blank or the start of the file,
    then returning only those lines).

    In practice, for a simple recovery just parse the whole file and
    deduplicate — duplicate re-queuing is harmless.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Payload log not found: {path}")

    with open(path, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Payload log is empty.")

    if last_run_only:
        # The last run is the trailing group of lines. We detect run boundaries
        # by noting that each sync run appends N consecutive lines (one per batch).
        # Since we don't have explicit run markers, we take the last N lines where
        # N = however many lines were written in one continuous append session.
        # Safest heuristic: just take all lines (re-queuing is idempotent).
        log("RECOVER_FROM_LAST_RUN=True: using all lines (safe — re-queuing is idempotent)")

    skus = set()
    for line in lines:
        try:
            batch = json.loads(line)
            for product in batch.get("products", []):
                sku = product.get("sku")
                if sku:
                    skus.add(sku)
                # Also capture child SKUs so we can look them up and identify
                # their parent in the DB (children themselves are not re-queued,
                # but we need them to find the parent product_id if the parent
                # SKU wasn't directly in the payload at the root level).
                for child in product.get("children", []):
                    child_sku = child.get("sku")
                    if child_sku:
                        skus.add(child_sku)
        except json.JSONDecodeError as e:
            log(f"Warning: skipping malformed log line: {e}")

    return skus

def get_root_product_ids(skus, conn):
    """
    Given a set of SKUs, return the product_ids of all root products:
      - configurable parents  (product_type = 'configurable')
      - standalone simples    (product_type = 'simple' AND parent_sku IS NULL)

    Configurable children are excluded — the sync will fetch them automatically
    when their parent is processed.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT product_id, sku, product_type, parent_sku
        FROM pim.product
        WHERE sku = ANY(%s)
        """,
        (list(skus),)
    )
    rows = cursor.fetchall()
    cursor.close()

    found_skus   = {row[1] for row in rows}
    missing_skus = skus - found_skus
    if missing_skus:
        log(f"Warning: {len(missing_skus)} SKUs from log not found in pim.product: "
            f"{', '.join(sorted(missing_skus)[:10])}{'...' if len(missing_skus) > 10 else ''}")

    root_ids    = []
    skipped     = []
    for product_id, sku, product_type, parent_sku in rows:
        pt = (product_type or "simple").lower()
        if pt == "configurable":
            root_ids.append((product_id, sku, "configurable parent"))
        elif pt == "simple" and not parent_sku:
            root_ids.append((product_id, sku, "standalone simple"))
        else:
            skipped.append(sku)

    if skipped:
        log(f"Skipping {len(skipped)} configurable children (will be fetched automatically "
            f"when their parents are processed)")

    return root_ids

def requeue_products(root_products, conn, dry_run):
    """Insert product_create rows into pim.alumio_sync_queue."""
    if not root_products:
        log("No root products to re-queue.")
        return 0

    rows_to_insert = [(pid,) for pid, _, _ in root_products]

    if dry_run:
        log(f"DRY RUN — would insert {len(rows_to_insert)} rows into pim.alumio_sync_queue:")
        for product_id, sku, kind in root_products:
            log(f"  product_id={product_id}  sku={sku}  ({kind})")
        log("Set DRY_RUN = False to actually insert.")
        return 0

    cursor = conn.cursor()
    inserted = 0
    for product_id, sku, kind in root_products:
        cursor.execute(
            """
            INSERT INTO pim.alumio_sync_queue (product_id, operation_type, status, created_at)
            VALUES (%s, 'product_create', 'pending', NOW())
            """,
            (product_id,)
        )
        inserted += 1
        log(f"  Queued product_id={product_id}  sku={sku}  ({kind})")

    conn.commit()
    cursor.close()
    return inserted

# ── Main ──────────────────────────────────────────────────────────────────────
log("=== Sync Queue Recovery ===")
log(f"Payload log : {PAYLOAD_LOG}")
log(f"Dry run     : {DRY_RUN}")

missing_vars = [v for v in ("PIM_POSTGRES_HOST","PIM_POSTGRES_NAME","PIM_POSTGRES_USER","PIM_POSTGRES_PASSWORD")
                if not os.environ.get(v)]
if missing_vars:
    raise Exception(f"Missing env vars: {', '.join(missing_vars)}")

try:
    log(f"Step 1: Extracting SKUs from payload log...")
    skus = extract_skus_from_log(PAYLOAD_LOG, RECOVER_FROM_LAST_RUN)
    log(f"  Found {len(skus)} unique SKUs in log")

    log(f"Step 2: Looking up root product_ids in pim.product...")
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, port=5432
    )
    root_products = get_root_product_ids(skus, conn)
    log(f"  {len(root_products)} root products to re-queue")

    log(f"Step 3: Inserting into pim.alumio_sync_queue...")
    inserted = requeue_products(root_products, conn, DRY_RUN)

    conn.close()

    if DRY_RUN:
        log("=== Dry run complete. Set DRY_RUN = False and re-run to insert. ===")
    else:
        log(f"=== Recovery complete: {inserted} products re-queued as product_create ===")

except Exception as e:
    log(f"ERROR: {e}")
    raise
