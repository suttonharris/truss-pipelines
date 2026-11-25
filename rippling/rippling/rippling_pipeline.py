import os
import sys

import requests
import dlt
from dlt.sources.rest_api import rest_api_source

DATASET = os.environ.get("DATASET", "raw_rippling")
API_KEY = os.environ["RIPPLING_API_KEY"]
BASE_URL = "https://rest.ripplingapis.com/"

# Optional: ONLY=companies,titles runs just those endpoints (for testing)
ONLY = [p.strip() for p in os.environ.get("ONLY", "").split(",") if p.strip()]

# Fields that must never be loaded, by table
BLOCKED_FIELDS = {
    "workers": ["race", "citizenship"],
}

# Endpoint -> write mode
ENDPOINTS = {
    "workers": "replace",
    "users": "replace",
    "companies": "replace",
    "legal-entities": "replace",
    "departments": "replace",
    "teams": "replace",
    "work-locations": "replace",
    "employment-types": "replace",
    "titles": "replace",
    "job-functions": "replace",
    "tracks": "replace",
    "levels": "replace",
    "job-codes": "replace",
    "job-dimensions": "replace",
    "business-partners": "replace",
    "supergroups": "replace",
    "custom-fields": "replace",
    "custom-objects": "replace",
    "object-categories": "replace",
    "entitlements": "replace",
    "time-entries": "merge",
    "time-cards": "merge",
    "shift-inputs": "merge",
    "leave-requests": "merge",
    "leave-balances": "replace",
    "leave-types": "replace",
    # Sensitive: delete these lines to exclude
    "compensations": "replace",
    "job-requisitions": "merge",
    "candidates": "merge",
}

HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def status_code(path):
    try:
        r = requests.get(BASE_URL + path, headers=HEADERS, timeout=30)
        return r.status_code
    except requests.RequestException:
        return "no response"


def root_cause(e):
    seen = set()
    while (e.__cause__ or e.__context__) and id(e) not in seen:
        seen.add(id(e))
        e = e.__cause__ or e.__context__
    return e


def strip_blocked(table):
    blocked = BLOCKED_FIELDS.get(table, [])

    def _strip(item):
        for field in blocked:
            item.pop(field, None)
        return item

    return _strip


def source_for(path, mode):
    table = path.replace("-", "_")
    resource = {
        "name": table,
        "write_disposition": mode,
        "endpoint": {"path": path, "data_selector": "results"},
    }
    if mode == "merge":
        resource["primary_key"] = "id"
    src = rest_api_source(
        {
            "client": {
                "base_url": BASE_URL,
                "auth": {"type": "bearer", "token": API_KEY},
                "paginator": {"type": "json_link", "next_url_path": "next_link"},
            },
            "resources": [resource],
        },
        name=f"rippling_{table}",
    )
    if table in BLOCKED_FIELDS:
        src.resources[table].add_map(strip_blocked(table))
    return src


pipeline = dlt.pipeline(
    pipeline_name="rippling",
    destination=dlt.destinations.bigquery(location="US"),
    dataset_name=DATASET,
)

to_run = {p: m for p, m in ENDPOINTS.items() if not ONLY or p in ONLY}

results = {}
for path, mode in to_run.items():
    code = status_code(path)
    if code != 200:
        results[path] = f"skipped (HTTP {code})"
        print(f"{path}: skipped (HTTP {code})")
        continue
    print(f"{path}: loading...")
    try:
        pipeline.run(source_for(path, mode))
        results[path] = "loaded"
    except Exception as e:
        cause = root_cause(e)
        msg = f"{type(cause).__name__}: {str(cause)[:400]}"
        print(f"\n!!! {path} FAILED: {msg}\n")
        results[path] = "FAILED (see error above)"
        pipeline.drop_pending_packages()

print("\n===== SUMMARY =====")
for path, outcome in results.items():
    print(f"{path:25} {outcome}")

if any(v.startswith("FAILED") for v in results.values()):
    sys.exit(1)
