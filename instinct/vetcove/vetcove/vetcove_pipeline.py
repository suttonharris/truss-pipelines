import time
from datetime import date, datetime, timedelta

import dlt
import requests

BASE = "https://shop.vetcove.com/api/corporates/external"
BACKFILL_START = date(2023, 1, 1)
LOOKBACK = timedelta(days=2)

session = requests.Session()
session.headers["X-API-KEY"] = dlt.secrets["vetcove_api_key"]


def get_pages(path, params=None):
    page = 1
    while True:
        r = session.get(f"{BASE}/{path}/",
                        params={**(params or {}), "page": page, "page_size": 100},
                        timeout=60)
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        body = r.json()
        results, pg = body["results"], body["paginator"]
        if pg["count"] > pg.get("max_count", pg["count"]):
            raise RuntimeError(f"{path} {params}: {pg['count']} rows exceeds cap - narrow the window")
        yield results
        if not results or pg["page_end_index"] >= pg["count"]:
            break
        page += 1
        time.sleep(0.7)


def month_windows(start, end):
    d = start.replace(day=1)
    while d <= end:
        nxt = (d + timedelta(days=32)).replace(day=1)
        yield d, min(nxt - timedelta(days=1), end)
        d = nxt


def fetch(path, cursor):
    if cursor.last_value == cursor.initial_value:
        for s, e in month_windows(BACKFILL_START, date.today()):
            yield from get_pages(path, {"start_date": s.isoformat(), "end_date": e.isoformat()})
    else:
        since = datetime.fromisoformat(cursor.last_value) - LOOKBACK
        yield from get_pages(path, {"start_last_activity": since.strftime("%Y-%m-%dT%H:%M:%S")})


@dlt.resource(primary_key="id", write_disposition="merge")
def orders(last_activity=dlt.sources.incremental("last_activity", initial_value="2000-01-01T00:00:00+00:00")):
    yield from fetch("orders", last_activity)


@dlt.resource(primary_key="id", write_disposition="merge")
def invoices(last_activity=dlt.sources.incremental("last_activity", initial_value="2000-01-01T00:00:00+00:00")):
    yield from fetch("invoices", last_activity)


@dlt.resource(primary_key="id", write_disposition="replace")
def buyers():
    yield from get_pages("buyers")


pipeline = dlt.pipeline(pipeline_name="vetcove", destination="bigquery", dataset_name="raw_vetcove_api")
print(pipeline.run([orders(), invoices(), buyers()]))
