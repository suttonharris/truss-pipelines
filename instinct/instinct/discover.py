import os
import requests

CID = os.environ["INSTINCT_CLIENT_ID"]
CSEC = os.environ["INSTINCT_CLIENT_SECRET"]

BASES = [
    "https://partner-us-east.instinctvet.com",
    "https://partner.instinctvet.com",
]


def try_token(base):
    url = f"{base}/v1/auth/token"
    attempts = [
        ("json body", lambda: requests.post(url, timeout=30, json={
            "grant_type": "client_credentials", "client_id": CID, "client_secret": CSEC})),
        ("json camelCase", lambda: requests.post(url, timeout=30, json={
            "grantType": "client_credentials", "clientId": CID, "clientSecret": CSEC})),
        ("form body", lambda: requests.post(url, timeout=30, data={
            "grant_type": "client_credentials", "client_id": CID, "client_secret": CSEC})),
        ("basic auth", lambda: requests.post(url, timeout=30, auth=(CID, CSEC),
            data={"grant_type": "client_credentials"})),
    ]
    for name, call in attempts:
        try:
            r = call()
        except requests.RequestException as e:
            print(f"  {base} ({name}): request error {type(e).__name__}")
            return None
        print(f"  {base} ({name}): HTTP {r.status_code}")
        if r.ok:
            body = r.json()
            print(f"    token response fields: {list(body.keys())}")
            return body.get("access_token") or body.get("accessToken") or body.get("token")
    return None


print("===== TOKEN =====")
BASE, TOKEN = None, None
for b in BASES:
    t = try_token(b)
    if t:
        BASE, TOKEN = b, t
        break
if not TOKEN:
    raise SystemExit("Could not get a token - send me the lines above.")
print(f"Using base URL: {BASE}")

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

PATHS = [
    "v1/locations", "v1/accounts", "v1/account-types", "v1/account-alerts",
    "v1/patients", "v1/alerts", "v1/appointments", "v1/appointment-types",
    "v1/visits", "v1/invoices", "v1/invoice-items", "v1/invoice-line-items",
    "v1/invoice-ledger-entries", "v1/treatments", "v1/visit-orders", "v1/orders",
    "v1/services", "v1/products", "v1/users", "v1/titles", "v1/species",
    "v1/breeds", "v1/sexes", "v1/reminders", "v1/reminder-labels",
    "v1/chart-documents", "v1/chart-templates", "v1/integrations",
    "v1/dispensed-prescriptions", "v1/prescriptions",
    "v1/payment-transactions", "v1/snippets",
]


def describe(path):
    url = f"{BASE}/{path}"
    r = requests.get(url, headers=HEADERS, params={"limit": 2}, timeout=30)
    if r.status_code in (400, 422):
        r = requests.get(url, headers=HEADERS, timeout=30)
    out = f"{path:30} HTTP {r.status_code}"
    if not r.ok:
        print(out)
        return
    try:
        body = r.json()
    except ValueError:
        print(out + "  (not JSON)")
        return
    if isinstance(body, list):
        out += "  [top level is a list]"
        if body and isinstance(body[0], dict):
            out += f"\n    record fields: {list(body[0].keys())[:30]}"
    elif isinstance(body, dict):
        out += f"\n    top-level fields: {list(body.keys())}"
        for k, v in body.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                out += f"\n    records in '{k}', fields: {list(v[0].keys())[:30]}"
            elif isinstance(v, dict):
                out += f"\n    '{k}' contains: {list(v.keys())}"
    print(out)


print("\n===== LOCATIONS VISIBLE TO THESE CREDENTIALS =====")
r = requests.get(f"{BASE}/v1/locations", headers=HEADERS, params={"limit": 100}, timeout=30)
if r.ok:
    body = r.json()
    rows = body if isinstance(body, list) else next(
        (v for v in body.values() if isinstance(v, list)), [])
    print(f"Count: {len(rows)}")
    for row in rows:
        if isinstance(row, dict):
            print(f"  id={row.get('id')}  name={row.get('name')}")
else:
    print(f"HTTP {r.status_code}")

print("\n===== ENDPOINTS =====")
for p in PATHS:
    try:
        describe(p)
    except requests.RequestException as e:
        print(f"{p:30} request error: {type(e).__name__}")
