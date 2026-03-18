#!/usr/bin/env python3
"""
App Store Connect Analytics — Diagnostic & Reset Tool
======================================================
Run this to diagnose why ASC reports are stuck at "pending",
clean up stale requests, and force-create fresh ONGOING requests.

Usage:
    cd ~/Downloads/clarity-dashboard   (or wherever your files are)
    python3 asc_debug.py
"""

import json, os, sys, time

try:
    import jwt as pyjwt
except ImportError:
    os.system("pip3 install PyJWT cryptography --break-system-packages")
    import jwt as pyjwt

try:
    import requests
except ImportError:
    os.system("pip3 install requests --break-system-packages")
    import requests

CONFIG_FILE = "config.json"
ASC_BASE    = "https://api.appstoreconnect.apple.com/v1"

# ── Helpers ─────────────────────────────────────────────────────────────────

def make_token(key_id, issuer_id, key_file):
    with open(key_file) as f:
        pk = f.read()
    now = int(time.time())
    return pyjwt.encode(
        {"iss": issuer_id, "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"},
        pk, algorithm="ES256", headers={"kid": key_id},
    )

def hdrs(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def sep(label=""):
    print("\n" + "─" * 60)
    if label:
        print(f"  {label}")
        print("─" * 60)

# ── Main ────────────────────────────────────────────────────────────────────

def diagnose_app(group, apple_id, key_id, issuer_id, key_file):
    sep(f"🍎  {group}  (apple_id={apple_id})")

    # ── Check key file ──────────────────────────────────────────────────────
    if not os.path.exists(key_file):
        print(f"  ❌  Key file not found: {key_file}")
        print(f"      Make sure {key_file} is in the same folder as this script.")
        return

    # ── Generate JWT ────────────────────────────────────────────────────────
    try:
        token = make_token(key_id, issuer_id, key_file)
        print(f"  ✅  JWT generated OK")
    except Exception as e:
        print(f"  ❌  JWT generation failed: {e}")
        return

    h = hdrs(token)

    # ── Test basic auth with /v1/apps ───────────────────────────────────────
    r = requests.get(f"{ASC_BASE}/apps/{apple_id}", headers=h, timeout=20)
    if r.status_code == 200:
        app_name = r.json().get("data", {}).get("attributes", {}).get("name", "?")
        print(f"  ✅  Auth OK — app name: {app_name}")
    elif r.status_code == 403:
        print(f"  ❌  HTTP 403 Forbidden — API key lacks permission to read this app.")
        print(f"      Check that the key has Admin or Developer role in App Store Connect.")
    elif r.status_code == 404:
        print(f"  ⚠️   HTTP 404 — app not found at apple_id={apple_id}")
        print(f"      Verify the apple_id in config.json matches the App Store Connect URL.")
    else:
        print(f"  ⚠️   /apps/{apple_id} returned HTTP {r.status_code}: {r.text[:200]}")

    # ── List all analytics report requests ──────────────────────────────────
    print(f"\n  📋  Listing analytics report requests...")
    r2 = requests.get(
        f"{ASC_BASE}/analyticsReportRequests",
        headers=h,
        params={"filter[app]": str(apple_id), "limit": 50},
        timeout=20,
    )
    print(f"      HTTP {r2.status_code}")

    if r2.status_code != 200:
        print(f"      Response: {r2.text[:400]}")
        if r2.status_code == 403:
            print("""
      ❌  403 on analyticsReportRequests — the API key does not have
          Analytics access. You need to:
          1. Go to App Store Connect → Users and Access → Integrations
             → App Store Connect API
          2. Revoke the current key
          3. Create a new key with Admin role (Analytics Reports requires Admin)
          4. Download the new .p8 file and update config.json / rename the file
""")
        return

    all_reqs = r2.json().get("data", [])
    print(f"      Found {len(all_reqs)} request(s)")

    stale_ids  = []
    active_ids = []
    for req in all_reqs:
        attrs  = req.get("attributes", {})
        req_id = req["id"]
        atype  = attrs.get("accessType", "?")
        stopped = attrs.get("stoppedDueToInactivity", False)
        print(f"      → id={req_id}  accessType={atype}  stoppedDueToInactivity={stopped}")
        if atype == "ONGOING":
            if stopped:
                stale_ids.append(req_id)
            else:
                active_ids.append(req_id)

    # ── Delete stale (stopped) requests ────────────────────────────────────
    if stale_ids:
        print(f"\n  🧹  Deleting {len(stale_ids)} stale (stopped) request(s)...")
        for req_id in stale_ids:
            rd = requests.delete(f"{ASC_BASE}/analyticsReportRequests/{req_id}",
                                 headers=h, timeout=20)
            if rd.status_code in (200, 204):
                print(f"      ✅  Deleted {req_id}")
            else:
                print(f"      ⚠️   Could not delete {req_id}: HTTP {rd.status_code} {rd.text[:100]}")
        active_ids = []   # force create fresh after deleting stale

    # ── Inspect active request (if any) ────────────────────────────────────
    if active_ids:
        req_id = active_ids[0]
        print(f"\n  🔍  Inspecting active request {req_id}...")

        # List reports (with and without filter)
        for label, params in [
            ("APP_USAGE filter", {"filter[reportType]": "APP_USAGE"}),
            ("no filter",        {}),
        ]:
            r3 = requests.get(
                f"{ASC_BASE}/analyticsReportRequests/{req_id}/reports",
                headers=h, params=params, timeout=20,
            )
            reports = r3.json().get("data", []) if r3.status_code == 200 else []
            print(f"      Reports ({label}): HTTP {r3.status_code} → {len(reports)} report(s)")
            for rep in reports:
                rat = rep.get("attributes", {})
                print(f"        → id={rep['id']}  type={rat.get('reportType')}  category={rat.get('reportCategory')}")

        if not any(
            r3.json().get("data", [])
            for r3 in [
                requests.get(f"{ASC_BASE}/analyticsReportRequests/{req_id}/reports",
                             headers=h, params={"filter[reportType]": "APP_USAGE"}, timeout=20),
                requests.get(f"{ASC_BASE}/analyticsReportRequests/{req_id}/reports",
                             headers=h, params={}, timeout=20),
            ]
        ):
            print("""
      ⏳  No reports available yet under the active request.
          This can happen for two reasons:
          a) The request is new (<48 hours) — keep waiting.
          b) The request is stuck — we'll delete it and create a new one below.
""")
            print(f"  🔄  Deleting stuck request {req_id} and creating a fresh one...")
            rd = requests.delete(f"{ASC_BASE}/analyticsReportRequests/{req_id}",
                                 headers=h, timeout=20)
            print(f"      Delete: HTTP {rd.status_code}")
            active_ids = []

    # ── Create a fresh ONGOING request if none active ───────────────────────
    if not active_ids:
        print(f"\n  🆕  Creating a new ONGOING analytics report request...")
        payload = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": "ONGOING"},
                "relationships": {
                    "app": {"data": {"type": "apps", "id": str(apple_id)}}
                },
            }
        }
        rc = requests.post(f"{ASC_BASE}/analyticsReportRequests",
                           headers=h, json=payload, timeout=20)
        print(f"      HTTP {rc.status_code}")
        if rc.status_code in (200, 201):
            new_id = rc.json().get("data", {}).get("id", "?")
            print(f"      ✅  Created request id={new_id}")
            print(f"      ⏳  Apple will generate reports within 24–48 hours.")
            print(f"          Run clarity_dashboard.py tomorrow to see data.")
        else:
            print(f"      ❌  Create failed: {rc.text[:400]}")
            if rc.status_code == 409:
                print("      (409 Conflict usually means a request already exists for this app)")
            if rc.status_code == 403:
                print("      (403 Forbidden — API key needs Admin role for Analytics Reports)")


def main():
    print("=" * 60)
    print("  App Store Connect — Diagnostic & Reset Tool")
    print("=" * 60)

    if not os.path.exists(CONFIG_FILE):
        print(f"\n❌  {CONFIG_FILE} not found. Run this from your clarity-dashboard folder.")
        sys.exit(1)

    config = json.load(open(CONFIG_FILE))
    apps   = config.get("appstore_connect", {}).get("apps", [])

    if not apps:
        print("\n❌  No apps found in config.json under 'appstore_connect'.")
        sys.exit(1)

    for app in apps:
        group     = app.get("group", "?")
        apple_id  = str(app.get("apple_id", "")).strip()
        key_id    = app.get("key_id", "").strip()
        issuer_id = app.get("issuer_id", "").strip()
        key_file  = app.get("key_file", "").strip()

        if not all([apple_id, key_id, issuer_id, key_file]):
            print(f"\n⏭  {group}: skipped (incomplete config)")
            continue

        try:
            diagnose_app(group, apple_id, key_id, issuer_id, key_file)
        except Exception as e:
            print(f"\n  ❌  Unexpected error for {group}: {e}")

    sep()
    print("  Done. Share the output above if you need help diagnosing further.")
    print("─" * 60 + "\n")


if __name__ == "__main__":
    main()
