import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import dlt
from dlt.sources.rest_api import rest_api_source

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))
DATASET = os.environ.get("DATASET", "raw_dialpad")
API_KEY = os.environ["DIALPAD_API_KEY"]
# Optional one-off backfill window, dates in Eastern time (YYYY-MM-DD). END is exclusive.
BACKFILL_START = os.environ.get("BACKFILL_START")
BACKFILL_END = os.environ.get("BACKFILL_END")

ET = ZoneInfo("America/New_York")

def et_midnight_ms(d):
    return int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=ET).timestamp() * 1000)

params = {}
if BACKFILL_START:
    params["started_after"] = et_midnight_ms(BACKFILL_START)
    if BACKFILL_END:
        params["started_before"] = et_midnight_ms(BACKFILL_END)
    print(f"Backfill window: {BACKFILL_START} to {BACKFILL_END or 'now'} (ET)")
else:
    params["started_after"] = int(
        (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp() * 1000
    )
    print(f"Nightly window: last {LOOKBACK_DAYS} days")

source = rest_api_source({
    "client": {
        "base_url": "https://dialpad.com/api/v2/",
        "auth": {"type": "bearer", "token": API_KEY},
        "paginator": {
            "type": "cursor",
            "cursor_path": "cursor",
            "cursor_param": "cursor",
        },
    },
    "resources": [
        {
            "name": "calls",
            "primary_key": "call_id",
            "write_disposition": "merge",
            "endpoint": {
                "path": "call",
                "data_selector": "items",
                "params": params,
            },
        },
    ],
})

pipeline = dlt.pipeline(
    pipeline_name="dialpad",
    destination=dlt.destinations.bigquery(location="US"),
    dataset_name=DATASET,
)

print("Starting Dialpad load...")
load_info = pipeline.run(source)
print(load_info)
