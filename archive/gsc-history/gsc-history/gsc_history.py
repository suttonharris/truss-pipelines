import os
import time
from datetime import date, timedelta
from urllib.parse import quote

import google.auth
import requests
import dlt
from google.auth.transport.requests import Request

SITE = os.environ.get("GSC_SITE", "https://www.trussvet.com/")
DATASET = os.environ.get("DATASET", "raw_search_console_history")
END = date.fromisoformat(os.environ.get("END_DATE", "2026-10-27"))
DAYS = int(os.environ.get("DAYS", "485"))  # about 16 months
START = END - timedelta(days=DAYS - 1)

creds, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
URL = ("https://searchconsole.googleapis.com/webmasters/v3/sites/"
       f"{quote(SITE, safe='')}/searchAnalytics/query")


def call(body):
    for attempt in range(6):
        if not creds.valid:
            creds.refresh(Request())
        r = requests.post(URL, json=body, timeout=120,
                          headers={"Authorization": f"Bearer {creds.token}"})
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(120, 2 ** attempt * 5))
            continue
        if not r.ok:
            raise SystemExit(f"Search Console API error {r.status_code}: {r.text[:300]}")
        return r.json()
    raise SystemExit("Search Console API kept failing - try again later.")


def day_rows(day, dims):
    start_row = 0
    while True:
        body = {
            "startDate": day.isoformat(), "endDate": day.isoformat(),
            "dimensions": dims, "type": "web", "dataState": "final",
            "rowLimit": 25000, "startRow": start_row,
        }
        rows = call(body).get("rows", [])
        for row in rows:
            rec = dict(zip(dims, row["keys"]))
            rec.update(data_date=day.isoformat(), clicks=row["clicks"],
                       impressions=row["impressions"], ctr=row["ctr"],
                       position=row["position"])
            yield rec
        if len(rows) < 25000:
            break
        start_row += 25000


def history(dims, label):
    def records():
        day = START
        n = 0
        while day <= END:
            batch = list(day_rows(day, dims))
            if batch:
                yield batch
            n += 1
            if n % 30 == 0:
                print(f"  {label}: through {day.isoformat()}")
            day += timedelta(days=1)
    return records


pipeline = dlt.pipeline(
    pipeline_name="gsc_history",
    destination=dlt.destinations.bigquery(location="US"),
    dataset_name=DATASET,
)

print(f"Search Console history for {SITE}: {START} to {END}")
pipeline.run(dlt.resource(history(["query", "country", "device"], "site_history"),
                          name="site_history", write_disposition="replace"))
print("site_history loaded")
pipeline.run(dlt.resource(history(["page", "query", "country", "device"], "url_history"),
                          name="url_history", write_disposition="replace"))
print("url_history loaded")
print("Done.")
