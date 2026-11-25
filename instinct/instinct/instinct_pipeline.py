import os
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from concurrent.futures import ThreadPoolExecutor

import requests
import dlt

BASE = "https://partner.instinctvet.com"
CID = os.environ["INSTINCT_CLIENT_ID"]
CSEC = os.environ["INSTINCT_CLIENT_SECRET"]
DATASET = os.environ.get("DATASET", "raw_instinct")
# Days of updated records to pull. "all" = full history (backfill).
LOOKBACK = os.environ.get("LOOKBACK_DAYS", "3")
# Optional: ONLY=invoices,visits runs just those tables
ONLY = [p.strip() for p in os.environ.get("ONLY", "").split(",") if p.strip()]
# Optional: cap per-record lookups (for testing)
CHILD_LIMIT = int(os.environ.get("CHILD_LIMIT", "0")) or None
# Full visits reload: always on backfill, on Sundays (ET), or if FULL_VISITS=1
FULL_VISITS = (
    LOOKBACK.lower() == "all"
    or os.environ.get("FULL_VISITS") == "1"
    or datetime.now(ZoneInfo("America/New_York")).weekday() == 6
)

REQUEST_TIMEOUT = 180          # seconds to wait for one response
PAGE_SIZES = {"v1/invoices": 50}  # smaller pages for slow endpoints

BLOCKED_FIELDS = {
    "users": ["deaLicenseNumber", "stateLicenseNumber",
              "stateControlledSubstancesLicenseNumber"],
}

# path -> (table name, write mode, filter by updatedSince?)
# Visits are handled separately (no date filter available).
ENDPOINTS = {
    # Reference data: full reload each run
    "v1/locations": ("locations", "replace", False),
    "v1/services": ("services", "replace", False),
    "v1/appointment-types": ("appointment_types", "replace", False),
    "v1/products": ("products", "replace", False),
    "v1/users": ("users", "replace", False),
    "v1/titles": ("titles", "replace", False),
    "v1/species": ("species", "replace", False),
    "v1/breeds": ("breeds", "replace", False),
    "v1/sexes": ("sexes", "replace", False),
    "v1/account-types": ("account_types", "replace", False),
    # Operational data: recent changes, merged by id
    "v1/accounts": ("accounts", "merge", True),
    "v1/patients": ("patients", "merge", True),
    "v1/appointments": ("appointments", "merge", True),
    "v1/invoices": ("invoices", "merge", True),
}


class InstinctClient:
    def __init__(self):
        self.token = None
        self.expires = 0

    def _authenticate(self):
        r = requests.post(f"{BASE}/v1/auth/token", timeout=60, json={
            "grant_type": "client_credentials",
            "client_id": CID,
            "client_secret": CSEC,
        })
        r.raise_for_status()
        body = r.json()
        self.token = body["access_token"]
        self.expires = time.time() + int(body.get("expires_in", 3600)) - 120

    def get(self, path, params):
        if not self.token or time.time() > self.expires:
            self._authenticate()
        last_error = None
        for attempt in range(6):
            try:
                r = requests.get(
                    f"{BASE}/{path}", params=params, timeout=REQUEST_TIMEOUT,
                    headers={"Authorization": f"Bearer {self.token}",
                             "Accept": "application/json"},
                )
            except (requests.Timeout, requests.ConnectionError) as e:
                last_error = e
                wait = min(120, 2 ** attempt * 5)
                print(f"  slow response on {path}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 401:
                self._authenticate()
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(120, 2 ** attempt * 5))
                continue
            r.raise_for_status()
            return r.json()
        if last_error:
            raise last_error
        r.raise_for_status()


client = InstinctClient()


def pages(path, params):
    params = dict(params, limit=PAGE_SIZES.get(path, 100))
    first = True
    while True:
        try:
            body = client.get(path, params)
        except requests.HTTPError as e:
            if first and e.response is not None and e.response.status_code in (400, 422):
                params.pop("limit", None)
                body = client.get(path, params)
            else:
                raise
        first = False
        rows = body.get("data", []) if isinstance(body, dict) else body
        if rows:
            yield rows
        cursor = (body.get("metadata") or {}).get("after") if isinstance(body, dict) else None
        if not cursor or not rows:
            break
        params["pageCursor"] = cursor
        params["pageDirection"] = "after"


def scrub(row, table):
    for field in BLOCKED_FIELDS.get(table, []):
        row.pop(field, None)


def list_resource(path, table, mode, params, capture=None):
    """Paginated list. capture = {field: [ ]} collects values (e.g. ids)."""
    def records():
        for rows in pages(path, params):
            for row in rows:
                scrub(row, table)
                if capture:
                    for field, bucket in capture.items():
                        if row.get(field) is not None:
                            bucket.append(row[field])
            yield rows

    kwargs = {"name": table, "write_disposition": mode}
    if mode == "merge":
        kwargs["primary_key"] = "id"
    return dlt.resource(records, **kwargs)


def by_id_resource(path_tpl, table, ids):
    """Fetch individual records by id (used for visits)."""
    def records():
        batch = []
        for i, rid in enumerate(ids, start=1):
            if i % 500 == 0:
                print(f"  {table}: {i} of {len(ids)} fetched")
            try:
                body = client.get(path_tpl.format(id=rid), {})
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    continue
                raise
            row = body.get("data", body) if isinstance(body, dict) else None
            if isinstance(row, dict):
                batch.append(row)
            if len(batch) >= 100:
                yield batch
                batch = []
        if batch:
            yield batch

    return dlt.resource(records, name=table, write_disposition="merge",
                        primary_key="id")


def _fetch_children(path_tpl, parent_col, pid):
    out = []
    try:
        for rows in pages(path_tpl.format(id=pid), {}):
            for row in rows:
                row[parent_col] = pid
            out.extend(rows)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
    return out


def child_resource(path_tpl, table, parent_col, parent_ids):
    # Fetch child lists per parent, several parents at a time
    workers = int(os.environ.get("PARALLEL", "8"))

    def records():
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for start in range(0, len(parent_ids), 500):
                chunk = parent_ids[start:start + 500]
                batch = []
                for rows in pool.map(
                        lambda pid: _fetch_children(path_tpl, parent_col, pid), chunk):
                    batch.extend(rows)
                done += len(chunk)
                print(f"  {table}: {done} of {len(parent_ids)} parents processed")
                if batch:
                    yield batch

    return dlt.resource(records, name=table, write_disposition="merge",
                        primary_key="id")


def root_cause(e):
    seen = set()
    while (e.__cause__ or e.__context__) and id(e) not in seen:
        seen.add(id(e))
        e = e.__cause__ or e.__context__
    return e


def run(resource, table, results):
    print(f"{table}: loading...")
    started = time.time()
    try:
        pipeline.run(resource, schema=dlt.Schema(f"instinct_{table}"))
        results[table] = f"loaded ({int(time.time() - started)}s)"
        return True
    except Exception as e:
        cause = root_cause(e)
        print(f"\n!!! {table} FAILED: {type(cause).__name__}: {str(cause)[:400]}\n")
        results[table] = "FAILED (see error above)"
        pipeline.abort_packages()
        return False


def unique(values, cap=None):
    out = list(dict.fromkeys(values))
    return out[:cap] if cap else out


def wanted(table):
    return not ONLY or table in ONLY


since = None
if LOOKBACK.lower() != "all":
    since = (datetime.now(timezone.utc) - timedelta(days=int(LOOKBACK))).strftime(
        "%Y-%m-%dT%H:%M:%SZ")

pipeline = dlt.pipeline(
    pipeline_name="instinct",
    destination=dlt.destinations.bigquery(location="US"),
    dataset_name=DATASET,
)

print(f"Instinct load: {'full history' if not since else 'updated since ' + since}")
print(f"Visits mode: {'full reload' if FULL_VISITS else 'recent, via invoices'}")

# Visits need invoices (for visit ids), so include invoices if visits requested
if ONLY and "visits" in ONLY and "invoices" not in ONLY:
    ONLY.append("invoices")

results = {}
invoice_ids, visit_ids_from_invoices = [], []

for path, (table, mode, filtered) in ENDPOINTS.items():
    if not wanted(table):
        continue
    params = {}
    if filtered and since:
        params["updatedSince"] = since
        try:
            client.get(path, dict(params, limit=1))
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 422):
                print(f"{table}: date filter not supported, pulling full table")
                params = {}
            else:
                raise

    capture = None
    if table == "invoices":
        capture = {"id": invoice_ids, "visitId": visit_ids_from_invoices}

    ok = run(list_resource(path, table, mode, params, capture), table, results)

    if ok and table == "invoices":
        ids = unique(invoice_ids, CHILD_LIMIT)
        print(f"invoice_line_items: fetching for {len(ids)} invoices")
        run(child_resource("v1/invoices/{id}/line-items", "invoice_line_items",
                           "invoice_id", ids), "invoice_line_items", results)

# ---- Visits ----
if wanted("visits"):
    if FULL_VISITS:
        run(list_resource("v1/visits", "visits", "merge", {}), "visits", results)
    else:
        ids = unique(visit_ids_from_invoices, CHILD_LIMIT)
        print(f"visits: fetching {len(ids)} visits by id (from recent invoices)")
        run(by_id_resource("v1/visits/{id}", "visits", ids), "visits", results)

print("\n===== SUMMARY =====")
for table, outcome in results.items():
    print(f"{table:26} {outcome}")

if any(v.startswith("FAILED") for v in results.values()):
    sys.exit(1)
