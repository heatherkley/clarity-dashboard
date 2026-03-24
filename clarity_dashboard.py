#!/usr/bin/env python3
"""
Microsoft Clarity Multi-Project Dashboard
==========================================
Pulls last 7 days of data from all your Clarity projects
and generates a clean HTML dashboard with iOS/Android grouped.

Usage:
    python3 clarity_dashboard.py
"""

import csv
import gzip
import io
import json
import os
import re
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
import subprocess

try:
    import jwt as pyjwt
except ImportError:
    print("Installing PyJWT...")
    os.system("pip3 install PyJWT cryptography --break-system-packages")
    import jwt as pyjwt
try:
    import weasyprint
    HAS_WEASYPRINT = "python"
except ImportError:
    # Fall back to CLI tool (e.g. installed via brew)
    _wc = subprocess.run(["which", "weasyprint"], capture_output=True, text=True)
    HAS_WEASYPRINT = "cli" if _wc.returncode == 0 else False

try:
    import requests
except ImportError:
    print("Installing required package: requests")
    os.system("pip3 install requests --break-system-packages")
    import requests

# ── Settings ───────────────────────────────────────────────────────────────────

CONFIG_FILE   = "config.json"
OUTPUT_FILE   = "clarity_dashboard.html"
OUTPUT_PDF    = "clarity_dashboard.pdf"
HISTORY_FILE  = "dashboard_history.json"
DAYS_BACK     = 30

CLARITY_API    = "https://www.clarity.ms/export-data/api/v1/project-live-insights"
REVENUECAT_API = "https://api.revenuecat.com/v1"

PLATFORM_COLORS = {
    "iOS":     "#007aff",
    "Android": "#3ddc84",
    "Website": "#f59e0b",
}

# ── Data Fetching ───────────────────────────────────────────────────────────────

def fetch_project(name, token, start_dt, end_dt):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {
        "startDate":   start_dt.strftime("%Y-%m-%d"),
        "endDate":     end_dt.strftime("%Y-%m-%d"),
        "granularity": "daily",
        "metrics":     "Sessions,Users,PagesPerSession,ScrollDepth,TotalSessionDuration"
    }
    try:
        r = requests.get(CLARITY_API, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        print(f"  ⚠️  {name}: HTTP {r.status_code} — {r.text[:200]}")
        return None
    except Exception as e:
        print(f"  ⚠️  {name}: {e}")
        return None

def safe_float(val, default=0):
    try:
        return round(float(val), 1)
    except (ValueError, TypeError):
        return default

def get_metric(raw_list, metric_name):
    """Extract the first info dict for a named metric from Clarity's response array."""
    if not isinstance(raw_list, list):
        return {}
    for item in raw_list:
        if item.get("metricName") == metric_name:
            info = item.get("information", [])
            return info[0] if info else {}
    return {}

def extract_clarity_metrics(raw):
    """Parse Clarity's array-of-metrics response into a flat dict."""
    if not isinstance(raw, list):
        return {}
    traffic    = get_metric(raw, "Traffic")
    engagement = get_metric(raw, "EngagementTime")

    # Device breakdown — array of {name, sessionsCount}
    device_info = []
    for item in raw:
        if item.get("metricName") == "Device":
            device_info = item.get("information", [])
            break
    total_sessions = safe_float(traffic.get("totalSessionCount", 0))
    # Normalize device names (Clarity returns "PC", "Mobile", "Tablet", "Desktop")
    DEVICE_NAME_NORM = {"Pc": "PC", "Mobile": "Mobile", "Tablet": "Tablet", "Desktop": "PC"}
    devices = {}
    for d in device_info:
        raw_name = d.get("name", "Unknown").capitalize()
        name  = DEVICE_NAME_NORM.get(raw_name, raw_name)
        count = safe_float(d.get("sessionsCount", 0))
        pct   = round((count / total_sessions * 100), 1) if total_sessions > 0 else 0
        devices[name] = {"count": int(count), "pct": pct}

    return {
        "sessions":         total_sessions,
        "users":            safe_float(traffic.get("distinctUserCount",          0)),
        "screensPerSession":safe_float(traffic.get("screensPerSessionPercentage",0)),
        "engagementSec":    safe_float(engagement.get("totalTime",               0)),
        "devices":          devices,
    }

def extract_clarity_daily(raw):
    """Extract per-day {date: {sessions, users}} from Clarity granularity=daily response."""
    if not isinstance(raw, list):
        return {}
    daily = {}
    for item in raw:
        if item.get("metricName") != "Traffic":
            continue
        info_items = item.get("information", [])
        for info in info_items:
            # Try all known Clarity date field names
            date_raw = (info.get("startDate") or info.get("date") or
                        info.get("timePeriod") or info.get("timestamp") or
                        info.get("period") or info.get("dateRange") or "")
            if not date_raw:
                continue
            try:
                date_key = str(date_raw)[:10]   # YYYY-MM-DD
                daily[date_key] = {
                    "sessions": safe_float(info.get("totalSessionCount", 0)),
                    "users":    safe_float(info.get("distinctUserCount",  0)),
                }
            except Exception:
                pass
    return dict(sorted(daily.items()))


# ── RevenueCat Fetching ─────────────────────────────────────────────────────────

def fetch_revenuecat(api_key, start_dt, end_dt):
    """Fetch overview metrics for a RevenueCat project via v2 API."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    v2 = "https://api.revenuecat.com/v2"
    try:
        # Step 1: look up the project_id for this API key
        r = requests.get(f"{v2}/projects", headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"    ⚠️  RevenueCat projects HTTP {r.status_code} — {r.text[:300]}")
            return None
        items = r.json().get("items", [])
        if not items:
            print("    ⚠️  RevenueCat: no projects found for this key")
            return None
        project_id = items[0]["id"]

        # Step 2: fetch metrics overview for the project
        params = {
            "start_time": start_dt.strftime("%Y-%m-%d"),
            "end_time":   end_dt.strftime("%Y-%m-%d"),
        }
        r2 = requests.get(
            f"{v2}/projects/{project_id}/metrics/overview",
            headers=headers, params=params, timeout=30
        )
        if r2.status_code == 200:
            return r2.json()
        print(f"    ⚠️  RevenueCat metrics HTTP {r2.status_code} — {r2.text[:300]}")
        return None
    except Exception as e:
        print(f"    ⚠️  RevenueCat error: {e}")
        return None

def extract_revenuecat_metrics(raw):
    """Pull key revenue metrics from RevenueCat v2 response (array of metric objects)."""
    if not raw:
        return {}

    # v2 returns {"metrics": [{"id": "...", "value": ...}, ...]}
    metric_list = raw.get("metrics", [])
    if isinstance(metric_list, list):
        by_id = {m.get("id", ""): m for m in metric_list}
        def mv(key): return safe_float((by_id.get(key) or {}).get("value", 0))
        return {
            "mrr":         mv("mrr"),
            "revenue":     mv("revenue") or mv("total_revenue"),
            "subscribers": mv("active_subscriptions") or mv("active_subscribers"),
            "trials":      mv("active_trials"),
        }

    # Flat dict fallback (v1 style)
    metrics = raw.get("overview", raw.get("data", raw))
    if isinstance(metrics, dict):
        return {
            "mrr":         safe_float(metrics.get("mrr",                  0)),
            "revenue":     safe_float(metrics.get("revenue",              metrics.get("total_revenue",   0))),
            "subscribers": safe_float(metrics.get("active_subscriptions", metrics.get("active_subscribers", 0))),
            "trials":      safe_float(metrics.get("active_trials",        0)),
        }
    return {}

def revenuecat_block(rc_data):
    """Build the revenue HTML block for a card."""
    if not rc_data:
        return ""
    # Accept either pre-combined metrics dict or raw API response
    if "_metrics" in rc_data:
        m = rc_data["_metrics"]
    else:
        m = extract_revenuecat_metrics(rc_data)
    mrr         = m.get("mrr", 0)
    revenue     = m.get("revenue", 0)
    subscribers = m.get("subscribers", 0)
    trials      = m.get("trials", 0)

    # If we got nothing meaningful, show raw for debugging
    if mrr == 0 and revenue == 0 and subscribers == 0:
        raw_json = json.dumps(rc_data, indent=2)[:2000]
        return (
            '<div class="rc-block rc-debug">'
            '<span class="rc-title">💰 RevenueCat — raw response (field names may need adjusting)</span>'
            '<div class="raw-toggle" onclick="toggleRaw(this)">▶ Show raw response</div>'
            '<pre class="raw-data" style="display:none">' + raw_json + "</pre>"
            "</div>"
        )

    mrr_str  = "${:,.2f}".format(mrr)
    rev_str  = "${:,.2f}".format(revenue)
    subs_str = "{:,}".format(int(subscribers))
    trial_str= "{:,}".format(int(trials))

    return (
        '<div class="rc-block">'
        '<span class="rc-title">💰 Revenue (last 30 days)</span>'
        '<div class="rc-metrics">'
        '<div class="rc-metric"><div class="rc-value">' + mrr_str  + '</div><div class="rc-label">MRR</div></div>'
        '<div class="rc-metric"><div class="rc-value">' + rev_str  + '</div><div class="rc-label">Revenue</div></div>'
        '<div class="rc-metric"><div class="rc-value">' + subs_str + '</div><div class="rc-label">Subscribers</div></div>'
        '<div class="rc-metric"><div class="rc-value">' + trial_str+ '</div><div class="rc-label">Trials</div></div>'
        "</div></div>"
    )

# ── App Store Connect Fetching ──────────────────────────────────────────────────

ASC_BASE = "https://api.appstoreconnect.apple.com/v1"

def make_asc_token(key_id, issuer_id, key_file):
    """Generate a short-lived JWT for App Store Connect API."""
    with open(key_file, "r") as f:
        private_key = f.read()
    now = int(time.time())
    token = pyjwt.encode(
        {"iss": issuer_id, "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"},
        private_key,
        algorithm="ES256",
        headers={"kid": key_id},
    )
    return token

ASC_CACHE_FILE = ".asc_cache.json"

def _asc_cache_load():
    """Load locally cached analytics request IDs (keyed by apple_id)."""
    if os.path.exists(ASC_CACHE_FILE):
        try:
            return json.load(open(ASC_CACHE_FILE))
        except Exception:
            pass
    return {}

def _asc_cache_save(cache):
    try:
        with open(ASC_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass

def _history_load():
    """Load the persistent dashboard history (daily snapshots for charts)."""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"clarity": {}, "revenuecat": {}}

def _history_save(history):
    """Save history file, keeping at most 365 entries per group (rolling year)."""
    try:
        for section in ("clarity", "revenuecat"):
            for gname, daily in history.get(section, {}).items():
                # Keep last 365 days only
                sorted_dates = sorted(daily.keys())
                if len(sorted_dates) > 365:
                    for old in sorted_dates[:-365]:
                        daily.pop(old, None)
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"    ⚠️  Could not save history: {e}")

def _history_append_revenuecat(history, group_name, metrics, date_key=None):
    """Append RevenueCat metrics for a group into the history (defaults to today)."""
    key = date_key or datetime.now().strftime("%Y-%m-%d")
    history.setdefault("revenuecat", {}).setdefault(group_name, {})[key] = {
        "mrr":         round(metrics.get("mrr",         0), 2),
        "revenue":     round(metrics.get("revenue",     0), 2),
        "subscribers": int(metrics.get("subscribers",   0)),
        "trials":      int(metrics.get("trials",        0)),
    }


def _history_append_clarity(history, group_name, sessions, users, date_key=None):
    """Append Clarity aggregate for a group into the history (defaults to today)."""
    key = date_key or datetime.now().strftime("%Y-%m-%d")
    history.setdefault("clarity", {}).setdefault(group_name, {})[key] = {
        "sessions": int(sessions),
        "users":    int(users),
    }


def _backfill_history(history, projects, rc_apps, days=30):
    """Back-fill up to `days` of daily data for any group missing history.

    Calls the Clarity and RevenueCat APIs once per day per group for each
    missing date.  This is a one-time catch-up that turns the dashboard into
    a real 30-day trend chart on first run.
    """
    today = datetime.now().date()

    # ── Clarity backfill ────────────────────────────────────────────────────────
    # Group projects by group name so we can aggregate per day
    groups_map = defaultdict(list)
    for proj in projects:
        token = proj.get("api_token", "").strip()
        if token and not token.startswith("PASTE_"):
            groups_map[proj.get("group", proj.get("name", ""))].append(proj)

    for gname, members in groups_map.items():
        existing = history.get("clarity", {}).get(gname, {})
        missing_days = []
        for offset in range(1, days + 1):          # yesterday → 30 days ago
            d = today - timedelta(days=offset)
            if d.strftime("%Y-%m-%d") not in existing:
                missing_days.append(d)

        if not missing_days:
            continue

        print(f"  📅 Clarity backfill for '{gname}': {len(missing_days)} days missing...")
        for day in missing_days:
            day_dt   = datetime(day.year, day.month, day.day)
            g_sess = g_users = 0
            for proj in members:
                raw = fetch_project(proj["name"], proj["api_token"], day_dt, day_dt)
                if raw:
                    m      = extract_clarity_metrics(raw)
                    g_sess += m.get("sessions", 0)
                    g_users += m.get("users",   0)
            if g_sess > 0 or g_users > 0:
                _history_append_clarity(history, gname, g_sess, g_users,
                                        date_key=day.strftime("%Y-%m-%d"))

    # ── RevenueCat backfill ──────────────────────────────────────────────────────
    for app in rc_apps:
        gname  = app.get("group", "")
        rc_key = app.get("api_key", "").strip()
        if not rc_key or rc_key.startswith("PASTE_"):
            continue
        existing = history.get("revenuecat", {}).get(gname, {})
        missing_days = []
        for offset in range(1, days + 1):
            d = today - timedelta(days=offset)
            if d.strftime("%Y-%m-%d") not in existing:
                missing_days.append(d)

        if not missing_days:
            continue

        print(f"  💰 RevenueCat backfill for '{gname}': {len(missing_days)} days missing...")
        for day in missing_days:
            day_dt = datetime(day.year, day.month, day.day)
            rc_raw = fetch_revenuecat(rc_key, day_dt, day_dt)
            if rc_raw:
                rc_m = rc_raw.get("_metrics") or extract_revenuecat_metrics(rc_raw)
                if rc_m.get("mrr") or rc_m.get("subscribers"):
                    _history_append_revenuecat(history, gname, rc_m,
                                               date_key=day.strftime("%Y-%m-%d"))


def _asc_create_request(apple_id, hdrs):
    """Create a new ONGOING analytics report request. Returns (id, is_new) or (None, False)."""
    payload = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {"app": {"data": {"type": "apps", "id": str(apple_id)}}},
        }
    }
    r = requests.post(f"{ASC_BASE}/analyticsReportRequests",
                      headers=hdrs, json=payload, timeout=30)
    print(f"    [ASC] Create request: HTTP {r.status_code}")
    if r.status_code in (200, 201):
        req_id = r.json().get("data", {}).get("id")
        print(f"    [ASC] Created id={req_id}")
        return req_id, True
    elif r.status_code == 409:
        print(f"    [ASC] 409 — a request already exists for this app (need Admin key to find it)")
    else:
        print(f"    [ASC] Create failed: {r.text[:300]}")
    return None, False

def fetch_appstore(apple_id, key_id, issuer_id, key_file):
    """Fetch iOS downloads/installs from App Store Connect Analytics Reports API.

    Works in two modes:
    - Admin key: can list all requests → always finds/reuses the right one
    - Limited key: uses a local .asc_cache.json to remember created request IDs
    """
    try:
        token  = make_asc_token(key_id, issuer_id, key_file)
        hdrs   = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        result = {}
        cache  = _asc_cache_load()
        cache_key = str(apple_id)

        # ── Step 1: find or create an ONGOING analytics report request ───────────
        ongoing_id = None

        # 1a. Try to list (works with Admin key)
        r = requests.get(
            f"{ASC_BASE}/analyticsReportRequests",
            headers=hdrs,
            params={"filter[app]": str(apple_id), "limit": 50},
            timeout=30,
        )
        print(f"    [ASC] Step1 list requests: HTTP {r.status_code}", end=" ")
        if r.status_code == 200:
            all_reqs = r.json().get("data", [])
            print(f"({len(all_reqs)} requests found)")
            stale_ids = []
            for req in all_reqs:
                attrs   = req.get("attributes", {})
                req_id  = req["id"]
                stopped = attrs.get("stoppedDueToInactivity", False)
                print(f"      → id={req_id} accessType={attrs.get('accessType')} stoppedDueToInactivity={stopped}")
                if attrs.get("accessType") == "ONGOING":
                    if not stopped and ongoing_id is None:
                        ongoing_id = req_id
                    elif stopped:
                        stale_ids.append(req_id)
            for stale_id in stale_ids:
                rd = requests.delete(f"{ASC_BASE}/analyticsReportRequests/{stale_id}",
                                     headers=hdrs, timeout=20)
                print(f"    [ASC] Deleted stale {stale_id}: HTTP {rd.status_code}")
        else:
            # 403 / limited key — fall back to cached ID
            print(f"(no list permission — using local cache)")
            cached_id = cache.get(cache_key)
            if cached_id:
                print(f"    [ASC] Checking cached request id={cached_id}...")
                ri = requests.get(f"{ASC_BASE}/analyticsReportRequests/{cached_id}",
                                  headers=hdrs, timeout=20)
                print(f"    [ASC] GET_INSTANCE: HTTP {ri.status_code}")
                if ri.status_code == 200:
                    attrs   = ri.json().get("data", {}).get("attributes", {})
                    stopped = attrs.get("stoppedDueToInactivity", False)
                    print(f"      stoppedDueToInactivity={stopped}")
                    if not stopped:
                        ongoing_id = cached_id
                    else:
                        # Stale — delete it and create fresh
                        print(f"    [ASC] Cached request is stale, deleting...")
                        requests.delete(f"{ASC_BASE}/analyticsReportRequests/{cached_id}",
                                        headers=hdrs, timeout=20)
                        cache.pop(cache_key, None)
                        _asc_cache_save(cache)
                elif ri.status_code == 404:
                    print(f"    [ASC] Cached request not found, clearing cache entry")
                    cache.pop(cache_key, None)
                    _asc_cache_save(cache)
            else:
                print(f"    [ASC] No cached request ID for apple_id={apple_id}")

        # 1b. Create if we still have no ID
        if not ongoing_id:
            new_id, is_new = _asc_create_request(apple_id, hdrs)
            if new_id:
                ongoing_id = new_id
                cache[cache_key] = new_id
                _asc_cache_save(cache)
                result["pending"] = True  # new request needs 24–48h to populate
            else:
                result["pending"] = True
                return result
        else:
            # Update cache with the confirmed good ID
            cache[cache_key] = ongoing_id
            _asc_cache_save(cache)

        print(f"    [ASC] Using request id={ongoing_id}")

        # ── Step 2: get reports from this request (try with and without filter) ───
        reports = []
        for type_filter in ({"filter[reportType]": "APP_USAGE"}, {}):
            r3 = requests.get(
                f"{ASC_BASE}/analyticsReportRequests/{ongoing_id}/reports",
                headers=hdrs,
                params=type_filter,
                timeout=30,
            )
            label = type_filter.get("filter[reportType]", "no filter")
            print(f"    [ASC] Step2 reports ({label}): HTTP {r3.status_code}", end=" ")
            if r3.status_code == 200:
                reports = r3.json().get("data", [])
                print(f"({len(reports)} reports)")
                for rep in reports:
                    print(f"      → id={rep['id']} type={rep.get('attributes',{}).get('reportType')} category={rep.get('attributes',{}).get('reportCategory')}")
                if reports:
                    break
            else:
                print(f"— {r3.text[:120]}")

        if not reports:
            # No reports yet — Apple needs up to 72h to generate data for a new request.
            # IMPORTANT: do NOT delete and recreate here — that resets the clock every run.
            # Just wait; the next run will find reports once Apple has generated them.
            print(f"    [ASC] No reports yet for request {ongoing_id} — still waiting on Apple (up to 72h)")
            result["pending"] = True
            return result

        # ── Step 3: get the latest instance ──────────────────────────────────────
        report_id = reports[0]["id"]
        instances = []
        for gran in ("MONTHLY", "DAILY"):
            r4 = requests.get(
                f"{ASC_BASE}/analyticsReports/{report_id}/instances",
                headers=hdrs,
                params={"filter[granularity]": gran},
                timeout=30,
            )
            print(f"    [ASC] Step3 instances ({gran}): HTTP {r4.status_code}", end=" ")
            if r4.status_code == 200:
                instances = r4.json().get("data", [])
                print(f"({len(instances)} instances)")
            else:
                print(f"— {r4.text[:200]}")
            if instances:
                break

        if not instances:
            result["pending"] = True
            return result

        # ── Step 4: get segment download URL ─────────────────────────────────────
        instance_id = instances[0]["id"]
        r5 = requests.get(
            f"{ASC_BASE}/analyticsReportInstances/{instance_id}/segments",
            headers=hdrs, timeout=30,
        )
        print(f"    [ASC] Step4 segments: HTTP {r5.status_code}")
        segments = r5.json().get("data", []) if r5.status_code == 200 else []
        if not segments:
            result["pending"] = True
            return result

        # ── Step 5: download & parse gzipped TSV ─────────────────────────────────
        dl_url = segments[0].get("attributes", {}).get("url", "")
        if not dl_url:
            result["pending"] = True
            return result

        r6 = requests.get(dl_url, headers=hdrs, timeout=60)
        if r6.status_code != 200:
            result["pending"] = True
            return result

        content = gzip.decompress(r6.content).decode("utf-8")
        reader  = csv.DictReader(io.StringIO(content), delimiter="\t")
        total_installs = 0
        daily_installs = {}
        for row in reader:
            val  = row.get("Installations") or row.get("First Time Downloads") or 0
            date = (row.get("Date") or row.get("date") or "").strip()[:10]
            try:
                count = int(float(str(val).replace(",", "") or 0))
                total_installs += count
                if date:
                    daily_installs[date] = daily_installs.get(date, 0) + count
            except (ValueError, TypeError):
                pass
        result["installs"]       = total_installs
        result["daily_installs"] = dict(sorted(daily_installs.items()))
        return result

    except Exception as e:
        print(f"    ⚠️  App Store Connect error: {e}")
        return None

def appstore_block(asc_data):
    """Build the App Store HTML block for a card."""
    if not asc_data:
        return ""
    if asc_data.get("pending"):
        return (
            '<div class="rc-block">'
            '<span class="rc-title">🍎 App Store</span>'
            '<div style="color:#94a3b8;font-size:0.8em;padding:4px 0">'
            'Apple is generating reports (can take up to 72h for a new request). '
            'Data will appear on the next dashboard run once ready.</div></div>'
        )
    installs = asc_data.get("installs")
    if installs is None:
        return ""
    return (
        '<div class="rc-block">'
        '<span class="rc-title">🍎 App Store (iOS)</span>'
        '<div class="rc-metrics">'
        f'<div class="rc-metric"><div class="rc-value">{int(installs):,}</div>'
        '<div class="rc-label">Downloads</div></div>'
        '</div></div>'
    )

# ── HTML Helpers ────────────────────────────────────────────────────────────────

def platform_badge(platform):
    color = PLATFORM_COLORS.get(platform, "#94a3b8")
    return f'<span class="badge" style="background:{color}">{platform}</span>'

def metric_block(label, value):
    return f"""<div class="metric">
              <div class="metric-value">{value}</div>
              <div class="metric-label">{label}</div>
            </div>"""

DEVICE_ICONS = {
    "Mobile":  "📱",
    "Tablet":  "⬛",
    "PC":      "🖥",
}
DEVICE_LABELS = {
    "Mobile":  "Mobile",
    "Tablet":  "Tablet",
    "PC":      "PC",
}

def platform_row(platform, data, raw):
    m        = extract_clarity_metrics(raw)
    sessions = m.get("sessions", 0)
    users    = m.get("users", 0)
    sps      = m.get("screensPerSession", 0)
    eng_s    = m.get("engagementSec", 0)
    eng_min  = round(eng_s / 60, 1) if eng_s else 0
    devices  = m.get("devices", {})
    color    = PLATFORM_COLORS.get(platform, "#94a3b8")
    has_data = sessions > 0
    dot_col  = "#22c55e" if has_data else "#e2e8f0"

    sessions_str = "{:,}".format(int(sessions))
    users_str    = "{:,}".format(int(users))
    sps_str      = "{}".format(round(sps, 1))
    eng_str      = "{}m".format(eng_min)
    status_title = "Data OK" if has_data else "No data"

    # Device breakdown pills
    device_pills = ""
    for dev_name in ["Mobile", "Tablet", "PC"]:
        if dev_name in devices:
            icon  = DEVICE_ICONS.get(dev_name, "•")
            label = DEVICE_LABELS.get(dev_name, dev_name)
            count = "{:,}".format(devices[dev_name]["count"])
            pct   = devices[dev_name]["pct"]
            device_pills += (
                '<span class="device-pill">'
                '{} {} <strong>{}</strong> '
                '<span class="device-pct">({}%)</span>'
                '</span>'
            ).format(icon, label, count, pct)

    device_block = ""
    if device_pills:
        device_block = '<div class="device-row">' + device_pills + '</div>'

    raw_block = ""
    if raw:
        raw_json = json.dumps(raw, indent=2)[:3000]
        raw_block = (
            '<div class="raw-toggle" onclick="toggleRaw(this)">▶ Show raw response</div>'
            '<pre class="raw-data" style="display:none">' + raw_json + "</pre>"
        )

    return f"""
      <div class="platform-row" style="border-left:3px solid {color}">
        <div class="platform-header">
          {platform_badge(platform)}
          <span class="status-dot" style="background:{dot_col}" title="{status_title}"></span>
        </div>
        <div class="metrics">
          {metric_block("Sessions", sessions_str)}
          {metric_block("Users", users_str)}
          {metric_block("Screens", sps_str)}
          {metric_block("Eng. Time", eng_str)}
        </div>
        {device_block}
        {raw_block}
      </div>"""

# ── HTML Generation ─────────────────────────────────────────────────────────────

def render_html(groups, rc_by_group, asc_by_group, total_projects, start_dt, end_dt, history=None):
    date_range = f"{start_dt.strftime('%b %d')} \u2013 {end_dt.strftime('%b %d, %Y')}"
    generated  = datetime.now().strftime("%b %d, %Y at %I:%M %p")
    history    = history or {}

    GROUP_ORDER = ["Shift", "Today's Front Pages", "Quiet Collection",
                   "P3", "Self Speak", "Footsteps with Jesus"]
    GROUP_COLORS = {
        "Shift":                "#3b82f6",
        "Today's Front Pages":  "#10b981",
        "Quiet Collection":     "#8b5cf6",
        "P3":                   "#f59e0b",
        "Self Speak":           "#ef4444",
        "Footsteps with Jesus": "#ec4899",
        "Lifeplus Pets":        "#06b6d4",
        "TeamBuildr Practice":  "#84cc16",
        "Shift Irish":          "#64748b",
    }
    PLATFORM_ORDER = {"iOS": 0, "Android": 1, "Website": 2}

    def gcolor(n):  return GROUP_COLORS.get(n, "#6b7280")
    def gsort(n):   return (GROUP_ORDER.index(n) if n in GROUP_ORDER else len(GROUP_ORDER), n)
    def tid(n):     return "tab-" + re.sub(r"[^a-z0-9]+", "-", n.lower()).strip("-")

    sorted_group_names = sorted(groups.keys(), key=gsort)

    # ── Build per-group chart data from history (accumulated weekly snapshots) ──
    # Clarity doesn't return time-series via its API — we build our own history
    # by saving each run's aggregate, giving us a growing trend over time.
    chart_data           = {}
    all_sessions_by_date = {}
    all_users_by_date    = {}
    clarity_history      = history.get("clarity",    {})
    rc_history           = history.get("revenuecat", {})

    for gname in sorted_group_names:
        # Sessions / users from accumulated history
        g_clarity  = clarity_history.get(gname, {})
        sess_dates = sorted(g_clarity.keys())
        for d in sess_dates:
            all_sessions_by_date[d] = all_sessions_by_date.get(d, 0) + g_clarity[d].get("sessions", 0)
            all_users_by_date[d]    = all_users_by_date.get(d, 0)    + g_clarity[d].get("users",    0)

        # ASC daily installs (from TSV — already per-day)
        asc_g      = asc_by_group.get(gname) or {}
        di         = asc_g.get("daily_installs") or {}
        inst_dates = sorted(di.keys())

        # RevenueCat history
        g_rc       = rc_history.get(gname, {})
        rc_dates   = sorted(g_rc.keys())

        chart_data[gname] = {
            "sessions":    {"dates": sess_dates,  "values": [g_clarity[d].get("sessions", 0) for d in sess_dates]},
            "users":       {"dates": sess_dates,  "values": [g_clarity[d].get("users",    0) for d in sess_dates]},
            "installs":    {"dates": inst_dates,  "values": [int(di[d])                       for d in inst_dates]},
            "mrr":         {"dates": rc_dates,    "values": [g_rc[d].get("mrr",         0)   for d in rc_dates]},
            "subscribers": {"dates": rc_dates,    "values": [g_rc[d].get("subscribers", 0)   for d in rc_dates]},
            "color":       gcolor(gname),
        }

    ov_dates = sorted(all_sessions_by_date.keys())
    chart_data["_overview"] = {
        "sessions": {"dates": ov_dates, "values": [int(all_sessions_by_date[d]) for d in ov_dates]},
        "users":    {"dates": ov_dates, "values": [int(all_users_by_date[d])    for d in ov_dates]},
    }
    chart_data_json = json.dumps(chart_data)

    # ── Summary totals ──────────────────────────────────────────────────────────
    all_results    = [p for g in groups.values() for p in g]
    total_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in all_results)
    total_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in all_results)
    total_mrr = total_revenue = 0.0
    for rc_data in rc_by_group.values():
        m = rc_data.get("_metrics") or extract_revenuecat_metrics(rc_data)
        total_mrr     += m.get("mrr",     0)
        total_revenue += m.get("revenue", 0)

    # ── Overview mini-cards ─────────────────────────────────────────────────────
    overview_cards_html = ""
    for gname in sorted_group_names:
        members    = groups[gname]
        g_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in members)
        g_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in members)
        asc_g      = asc_by_group.get(gname) or {}
        installs   = asc_g.get("installs")
        pending    = asc_g.get("pending", False)
        rc         = rc_by_group.get(gname)
        rc_m       = (rc.get("_metrics") or extract_revenuecat_metrics(rc)) if rc else {}
        mrr        = rc_m.get("mrr", 0)
        subs       = rc_m.get("subscribers", 0)
        color      = gcolor(gname)
        tab_target = tid(gname)

        inst_str = ""
        if pending:
            inst_str = "<span style='color:#64748b;font-size:0.72em'>⏳ iOS pending (up to 72h)</span>"
        elif installs is not None:
            inst_str = f"<b>{int(installs):,}</b> <span class='ov-sub'>iOS installs</span>"

        rc_line = ""
        if mrr:
            rc_line = f"<div class='ov-rc'>${mrr:,.0f} MRR &nbsp;·&nbsp; {int(subs):,} subs</div>"

        spark_el = ""  # no charts/bars on overview — numbers only

        overview_cards_html += f"""
      <div class="ov-card" onclick="showTab('{tab_target}')" style="--accent:{color}">
        <div class="ov-name" style="color:{color}">{gname}</div>
        <div class="ov-stats">
          <div><b>{int(g_sessions):,}</b><span class="ov-sub"> sessions</span></div>
          <div><b>{int(g_users):,}</b><span class="ov-sub"> users</span></div>
          {"<div>" + inst_str + "</div>" if inst_str else ""}
        </div>
        {rc_line}
        {spark_el}
        <div class="ov-tap">tap to explore &rarr;</div>
      </div>"""

    # ── Per-group tab buttons + content ────────────────────────────────────────
    tabs_nav_html  = '<button class="tab-btn active" id="btn-tab-overview" onclick="showTab(\'tab-overview\')">Overview</button>\n'
    tabs_body_html = f"""
<div id="tab-overview" class="tab-pane">
  <div class="ov-grid">{overview_cards_html}
  </div>
</div>"""

    for gname in sorted_group_names:
        members    = groups[gname]
        t_id       = tid(gname)
        color      = gcolor(gname)
        g_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in members)
        g_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in members)
        asc_g      = asc_by_group.get(gname) or {}
        installs   = asc_g.get("installs")
        pending    = asc_g.get("pending", False)
        rc         = rc_by_group.get(gname)
        rc_m       = (rc.get("_metrics") or extract_revenuecat_metrics(rc)) if rc else {}
        mrr        = rc_m.get("mrr", 0)
        revenue    = rc_m.get("revenue", 0)
        subs       = rc_m.get("subscribers", 0)
        trials     = rc_m.get("trials", 0)
        gcd          = chart_data.get(gname, {})
        has_sessions = bool(gcd.get("sessions", {}).get("dates"))
        has_installs = bool(gcd.get("installs", {}).get("dates"))
        has_rc_trend = bool(gcd.get("mrr",      {}).get("dates"))

        tabs_nav_html += f'<button class="tab-btn" id="btn-{t_id}" onclick="showTab(\'{t_id}\')">{gname}</button>\n'

        # KPI bar
        kpi_items = [
            (f"{int(g_sessions):,}", "Sessions"),
            (f"{int(g_users):,}",    "Users"),
        ]
        if pending:
            kpi_items.append(("⏳", "iOS (initializing)"))
        elif installs is not None:
            kpi_items.append((f"{int(installs):,}", "iOS Installs"))
        if mrr:     kpi_items.append((f"${mrr:,.0f}",    "MRR"))
        if revenue: kpi_items.append((f"${revenue:,.2f}", "Revenue"))
        if subs:    kpi_items.append((f"{int(subs):,}",  "Subscribers"))
        if trials:  kpi_items.append((f"{int(trials):,}", "Trials"))

        kpi_html = "".join(
            f'<div class="kpi"><div class="kpi-val">{v}</div><div class="kpi-lbl">{l}</div></div>'
            for v, l in kpi_items
        )

        # Charts row — sessions/users trend, iOS downloads, MRR/subscribers
        no_data_note = (
            '<div class="chart-no-data">Building trend data \u2014 charts grow with each weekly run</div>'
            if not has_sessions and not has_installs and not has_rc_trend else ""
        )
        charts_row = f"""
    <div class="charts-row">
      <div class="chart-card">
        <div class="chart-title">Sessions &amp; Users Over Time</div>
        {no_data_note}
        <canvas id="chart-{t_id}-sessions" {"style='display:none'" if not has_sessions else ""}></canvas>
      </div>"""
        if has_installs or asc_g.get("pending"):
            inst_note = '<div class="chart-no-data">iOS download trend — initializing first report</div>' if not has_installs else ""
            charts_row += f"""
      <div class="chart-card">
        <div class="chart-title">Daily iOS Downloads</div>
        {inst_note}
        <canvas id="chart-{t_id}-installs" {"style='display:none'" if not has_installs else ""}></canvas>
      </div>"""
        if has_rc_trend or mrr:
            rc_note = '<div class="chart-no-data">Revenue trend \u2014 grows with each weekly run</div>' if not has_rc_trend else ""
            charts_row += f"""
      <div class="chart-card">
        <div class="chart-title">MRR &amp; Subscribers Over Time</div>
        {rc_note}
        <canvas id="chart-{t_id}-revenue" {"style='display:none'" if not has_rc_trend else ""}></canvas>
      </div>"""
        charts_row += "\n    </div>"

        # Platform detail card
        platform_rows_html = ""
        for r in sorted(members, key=lambda x: PLATFORM_ORDER.get(x["platform"], 99)):
            platform_rows_html += platform_row(r["platform"], r["data"], r["raw"])

        combined_block = ""
        if len(members) > 1:
            combined_block = f"""
          <div class="combined-total">
            <span class="combined-label">Combined</span>
            <span class="combined-stat">{int(g_sessions):,} sessions</span>
            <span class="combined-sep">&middot;</span>
            <span class="combined-stat">{int(g_users):,} users</span>
          </div>"""

        rc_html_block  = revenuecat_block(rc)
        asc_html_block = appstore_block(asc_g)

        tabs_body_html += f"""
<div id="{t_id}" class="tab-pane hidden">
  <div class="client-kpi-bar" style="border-top:3px solid {color}">
    {kpi_html}
  </div>
  {charts_row}
  <div class="grid">
    <div class="card">
      <div class="card-header">
        <span class="project-name">{gname}</span>
        <span class="platform-count">{len(members)} platform{"s" if len(members)!=1 else ""}</span>
      </div>
      {combined_block}
      {platform_rows_html}
      {rc_html_block}
      {asc_html_block}
    </div>
  </div>
</div>"""

    # ── Final HTML assembly ─────────────────────────────────────────────────────
    mrr_str = "${:,.0f}".format(total_mrr)
    rev_str = "${:,.0f}".format(total_revenue)
    rc_summary = (
        f'<div class="summary-item"><div class="summary-value">{mrr_str}</div>'
        f'<div class="summary-label">Total MRR</div></div>'
        f'<div class="summary-item"><div class="summary-value">{rev_str}</div>'
        f'<div class="summary-label">Total Revenue</div></div>'
    ) if rc_by_group else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="dark">
  <title>3Advance Analytics \u2014 {date_range}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    /* ── Base ── */
    body{{font-family:'Inter',system-ui,sans-serif;background:#060d1a;color:#e2e8f0;min-height:100vh;position:relative}}
    body::before{{content:'';position:fixed;inset:0;background-image:radial-gradient(rgba(56,189,248,0.04) 1px,transparent 1px);background-size:28px 28px;pointer-events:none;z-index:0}}
    canvas{{background:transparent!important}}
    header,.summary,.tabs-nav,.tab-pane,.grid,.charts-row,.ov-grid,.client-kpi-bar,footer{{position:relative;z-index:1}}
    /* ── Header ── */
    header{{background:linear-gradient(135deg,#070e1c 0%,#0c1a35 60%,#070e1c 100%);border-bottom:1px solid rgba(56,189,248,0.12);padding:20px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;position:relative;overflow:hidden}}
    header::after{{content:'';position:absolute;top:0;left:15%;right:15%;height:1px;background:linear-gradient(90deg,transparent,rgba(56,189,248,0.6),transparent)}}
    header h1{{font-size:1.25rem;font-weight:700;color:white;letter-spacing:-0.01em}}
    header h1 span{{background:linear-gradient(90deg,#38bdf8,#818cf8);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
    .meta{{font-size:0.8rem;color:#475569}}
    /* ── Summary bar ── */
    .summary{{display:flex;flex-wrap:wrap;background:rgba(255,255,255,0.02);border-bottom:1px solid rgba(255,255,255,0.05)}}
    .summary-item{{flex:1;min-width:130px;padding:18px 24px;border-right:1px solid rgba(255,255,255,0.05)}}
    .summary-item:last-child{{border-right:none}}
    .summary-value{{font-size:1.8rem;font-weight:700;color:white;letter-spacing:-0.02em}}
    .summary-label{{font-size:0.68rem;color:#475569;margin-top:3px;text-transform:uppercase;letter-spacing:0.06em}}
    /* ── Tabs nav ── */
    .tabs-nav{{padding:0 32px;display:flex;gap:0;overflow-x:auto;scrollbar-width:none;border-bottom:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.01)}}
    .tabs-nav::-webkit-scrollbar{{display:none}}
    .tab-btn{{background:none;border:none;color:#475569;padding:14px 20px;font-size:0.82rem;font-weight:500;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;transition:all 0.2s;letter-spacing:0.02em;font-family:inherit}}
    .tab-btn:hover{{color:#94a3b8}}
    .tab-btn.active{{color:#38bdf8;border-bottom-color:#38bdf8;text-shadow:0 0 20px rgba(56,189,248,0.4)}}
    /* ── Tab panes ── */
    .tab-pane{{display:block}}
    .tab-pane.hidden{{display:none}}
    /* ── KPI bar per client ── */
    .client-kpi-bar{{display:flex;flex-wrap:wrap;background:rgba(255,255,255,0.015);padding:20px 32px;gap:8px;border-bottom:1px solid rgba(255,255,255,0.05);border-top:3px solid var(--accent,#38bdf8)}}
    .kpi{{flex:1;min-width:110px;text-align:center;padding:12px 8px}}
    .kpi-val{{font-size:2rem;font-weight:700;color:white;letter-spacing:-0.02em}}
    .kpi-lbl{{font-size:0.65rem;color:#475569;text-transform:uppercase;letter-spacing:0.07em;margin-top:4px}}
    /* ── Charts ── */
    .charts-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;padding:20px 32px 0}}
    .chart-card{{background:rgba(255,255,255,0.025);border:1px solid rgba(255,255,255,0.07);border-radius:16px;padding:20px 22px;backdrop-filter:blur(8px);transition:border-color 0.2s,box-shadow 0.2s}}
    .chart-card:hover{{border-color:rgba(56,189,248,0.2);box-shadow:0 0 32px rgba(56,189,248,0.06)}}
    .chart-title{{font-size:0.72rem;font-weight:600;color:#475569;text-transform:uppercase;letter-spacing:0.09em;margin-bottom:16px}}
    .chart-no-data{{font-size:0.8rem;color:#1e3a4a;padding:32px 0;text-align:center;font-style:italic}}
    /* ── Overview grid ── */
    .ov-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px;padding:20px 32px}}
    .ov-card{{background:rgba(255,255,255,0.025);border:1px solid rgba(255,255,255,0.07);border-radius:16px;padding:18px 20px;cursor:pointer;transition:all 0.2s;position:relative;overflow:hidden}}
    .ov-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--accent,#3b82f6),transparent)}}
    .ov-card:hover{{transform:translateY(-2px);border-color:rgba(56,189,248,0.2);box-shadow:0 8px 40px rgba(56,189,248,0.07)}}
    .ov-name{{font-weight:700;font-size:1rem;margin-bottom:10px}}
    .ov-stats{{display:flex;gap:16px;font-size:0.85rem;flex-wrap:wrap;color:#e2e8f0}}
    .ov-sub{{color:#334155;font-size:0.78em}}
    .ov-rc{{margin-top:8px;font-size:0.78rem;color:#34d399;background:rgba(52,211,153,0.08);border-radius:6px;padding:4px 10px;border:1px solid rgba(52,211,153,0.12)}}
    .ov-tap{{font-size:0.68rem;color:#1e3a4a;margin-top:8px;text-align:right}}
    /* ── Detail grid ── */
    .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(400px,1fr));gap:20px;padding:20px 32px 32px;max-width:1600px;margin:0 auto}}
    .card{{background:rgba(255,255,255,0.025);border:1px solid rgba(255,255,255,0.07);border-radius:16px;padding:20px 24px}}
    .card-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid rgba(255,255,255,0.06)}}
    .project-name{{font-weight:700;font-size:1rem;color:white}}
    .platform-count{{font-size:0.75rem;color:#334155}}
    .combined-total{{display:flex;align-items:center;gap:8px;background:rgba(255,255,255,0.04);border-radius:8px;padding:8px 12px;margin-bottom:12px;font-size:0.82rem;border:1px solid rgba(255,255,255,0.06)}}
    .combined-label{{font-weight:600;color:#64748b}}
    .combined-stat{{color:#e2e8f0;font-weight:500}}
    .combined-sep{{color:#1e3a4a}}
    .platform-row{{border-radius:10px;padding:12px 12px 12px 14px;margin-bottom:10px;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.05)}}
    .platform-row:last-child{{margin-bottom:0}}
    .platform-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}}
    .badge{{color:white;font-size:0.68rem;font-weight:600;padding:3px 8px;border-radius:5px;text-transform:uppercase;letter-spacing:0.05em}}
    .status-dot{{width:7px;height:7px;border-radius:50%}}
    .metrics{{display:flex;gap:6px;flex-wrap:wrap}}
    .metric{{flex:1;min-width:76px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.07);border-radius:8px;padding:8px 10px;text-align:center}}
    .metric-value{{font-size:1.1rem;font-weight:700;color:white}}
    .metric-label{{font-size:0.62rem;color:#475569;margin-top:2px;text-transform:uppercase;letter-spacing:0.03em;white-space:nowrap}}
    .device-row{{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,255,255,0.06)}}
    .device-pill{{font-size:0.75rem;color:#64748b;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:20px;padding:4px 10px}}
    .device-pill strong{{color:#e2e8f0}}
    .device-pct{{color:#334155;font-size:0.7rem}}
    .rc-block{{margin-top:12px;padding:12px 14px;background:rgba(52,211,153,0.05);border:1px solid rgba(52,211,153,0.12);border-radius:12px}}
    .rc-block.rc-debug{{background:rgba(251,191,36,0.05);border-color:rgba(251,191,36,0.15)}}
    .rc-title{{font-size:0.72rem;font-weight:600;color:#34d399;display:block;margin-bottom:8px}}
    .rc-block.rc-debug .rc-title{{color:#fbbf24}}
    .rc-metrics{{display:flex;gap:8px;flex-wrap:wrap}}
    .rc-metric{{flex:1;min-width:72px;background:rgba(255,255,255,0.04);border-radius:8px;padding:7px 10px;text-align:center;border:1px solid rgba(52,211,153,0.08)}}
    .rc-value{{font-size:1rem;font-weight:700;color:#34d399}}
    .rc-label{{font-size:0.62rem;color:#064e3b;margin-top:2px;text-transform:uppercase;letter-spacing:0.05em}}
    .raw-toggle{{margin-top:10px;font-size:0.7rem;color:#1e3a4a;cursor:pointer;user-select:none}}
    .raw-toggle:hover{{color:#38bdf8}}
    .raw-data{{margin-top:6px;background:rgba(0,0,0,0.3);border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:10px;font-size:0.68rem;overflow-x:auto;color:#475569;white-space:pre-wrap;word-break:break-all}}
    footer{{text-align:center;padding:24px;font-size:0.78rem;color:#1e3a4a}}
    code{{background:rgba(56,189,248,0.08);padding:2px 6px;border-radius:4px;color:#38bdf8}}
  </style>
</head>
<body>

<header>
  <div>
    <h1>&#x1F4CA; 3Advance <span>Analytics</span></h1>
    <div class="meta">Last 30 days &nbsp;&middot;&nbsp; {date_range}</div>
  </div>
  <div class="meta" style="text-align:right">Generated {generated}<br><span style="color:#1e3a4a;font-size:0.75rem">Auto-updates every Monday</span></div>
</header>

<div class="summary">
  <div class="summary-item">
    <div class="summary-value">{len(groups)}</div>
    <div class="summary-label">Apps / Sites</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{total_projects}</div>
    <div class="summary-label">Platforms</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{int(total_sessions):,}</div>
    <div class="summary-label">Total Sessions</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{int(total_users):,}</div>
    <div class="summary-label">Total Users</div>
  </div>
  {rc_summary}
</div>

<div class="tabs-nav">
{tabs_nav_html}
</div>

{tabs_body_html}

<footer>3Advance Analytics &middot; Auto-updated weekly via GitHub Actions</footer>

<script>
const CD = {chart_data_json};

function hexToRgb(hex) {{
  const r = parseInt(hex.slice(1,3),16), g = parseInt(hex.slice(3,5),16), b = parseInt(hex.slice(5,7),16);
  return `${{r}},${{g}},${{b}}`;
}}

function makeBarChart(canvasId, labels, values, colors, label) {{
  const el = document.getElementById(canvasId);
  if (!el || !labels.length) return null;
  const ctx = el.getContext('2d');
  return new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label,
        data: values,
        backgroundColor: colors.map(c => `rgba(${{hexToRgb(c)}},0.65)`),
        borderColor: colors,
        borderWidth: 1,
        borderRadius: 5,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{ backgroundColor:'rgba(6,13,26,0.92)', titleColor:'#e2e8f0', bodyColor:'#94a3b8', borderColor:'rgba(56,189,248,0.2)', borderWidth:1 }},
      }},
      scales: {{
        x: {{ ticks: {{ font: {{ size: 10 }}, color:'#475569', maxRotation: 35, minRotation: 0 }}, grid: {{ display:false }} }},
        y: {{ ticks: {{ font: {{ size: 11 }}, color:'#334155' }}, grid: {{ color:'rgba(255,255,255,0.04)' }}, beginAtZero:true }},
      }},
    }},
  }});
}}

function makeLineChart(canvasId, labels, datasets, opts) {{
  const el = document.getElementById(canvasId);
  if (!el || !labels.length) return null;
  const ctx = el.getContext('2d');
  const dsets = datasets.map(d => {{
    const grad = ctx.createLinearGradient(0, 0, 0, opts && opts.spark ? 44 : 200);
    grad.addColorStop(0, `rgba(${{hexToRgb(d.color)}},0.18)`);
    grad.addColorStop(1, `rgba(${{hexToRgb(d.color)}},0)`);
    return {{
      label: d.label,
      data: d.values,
      borderColor: d.color,
      backgroundColor: grad,
      borderWidth: opts && opts.spark ? 1.5 : 2,
      pointRadius: (opts && opts.spark) ? 0 : (labels.length > 14 ? 0 : 3),
      pointHoverRadius: opts && opts.spark ? 0 : 5,
      pointBackgroundColor: d.color,
      fill: true,
      tension: 0.4,
    }};
  }});
  return new Chart(ctx, {{
    type: 'line',
    data: {{ labels, datasets: dsets }},
    options: {{
      responsive: true,
      maintainAspectRatio: opts && opts.spark ? false : true,
      plugins: {{
        legend: {{ display: !opts?.spark && dsets.length > 1, labels: {{ boxWidth: 8, font: {{ size: 11 }}, color:'#64748b' }} }},
        tooltip: {{ mode: 'index', intersect: false, backgroundColor:'rgba(6,13,26,0.9)', titleColor:'#e2e8f0', bodyColor:'#94a3b8', borderColor:'rgba(56,189,248,0.2)', borderWidth:1 }},
      }},
      scales: opts && opts.spark ? {{ x: {{ display:false }}, y: {{ display:false }} }} : {{
        x: {{ ticks: {{ maxTicksLimit: 8, font: {{ size: 11 }}, color:'#334155' }}, grid: {{ color:'rgba(255,255,255,0.04)' }} }},
        y: {{ ticks: {{ font: {{ size: 11 }}, color:'#334155' }}, grid: {{ color:'rgba(255,255,255,0.04)' }}, beginAtZero: true }},
      }},
      interaction: {{ mode: 'nearest', axis: 'x', intersect: false }},
    }},
  }});
}}

function initCharts() {{
  const ov = CD['_overview'] || {{}};
  // Overview: per-group colored lines — real 30-day daily data from backfill
  const ovGroups = Object.keys(CD).filter(k => !k.startsWith('_'));
  if (ovGroups.length) {{
    const sessDsets = ovGroups
      .filter(g => CD[g].sessions && CD[g].sessions.dates.length > 0)
      .map(g => ({{ label: g, values: CD[g].sessions.values, color: CD[g].color || '#6b7280' }}));
    const userDsets = ovGroups
      .filter(g => CD[g].users && CD[g].users.dates.length > 0)
      .map(g => ({{ label: g, values: CD[g].users.values,    color: CD[g].color || '#6b7280' }}));
    // All groups share the same date labels (union, sorted)
    const allDates = [...new Set(ovGroups.flatMap(g => (CD[g].sessions||{{}}).dates||[]))].sort();
    // Align each group's values to allDates (null for missing days)
    function alignToAllDates(g, field) {{
      const map = {{}};
      const series = CD[g][field] || {{}};
      (series.dates||[]).forEach((d,i) => {{ map[d] = series.values[i]; }});
      return allDates.map(d => map[d] !== undefined ? map[d] : null);
    }}
    const sessDs = ovGroups.map(g => ({{ label:g, values: alignToAllDates(g,'sessions'), color: CD[g].color||'#6b7280' }}));
    const userDs = ovGroups.map(g => ({{ label:g, values: alignToAllDates(g,'users'),    color: CD[g].color||'#6b7280' }}));
    makeLineChart('chart-ov-sessions', allDates, sessDs);
    makeLineChart('chart-ov-users',    allDates, userDs);
  }}

  for (const [gname, gd] of Object.entries(CD)) {{
    if (gname.startsWith('_')) continue;
    const tabId  = 'tab-' + gname.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g,'');
    const color  = gd.color || '#6b7280';
    // Spark (overview card) — only if canvas exists and we have ≥2 points
    const sparkId = 'spark-' + tabId;
    if (gd.sessions && gd.sessions.dates.length >= 2 && document.getElementById(sparkId))
      makeLineChart(sparkId, gd.sessions.dates, [
        {{ label:'Sessions', values: gd.sessions.values, color }}
      ], {{ spark: true }});
    // Sessions + Users dual line
    const sessId = `chart-${{tabId}}-sessions`;
    if (gd.sessions && gd.sessions.dates.length)
      makeLineChart(sessId, gd.sessions.dates, [
        {{ label:'Sessions', values: gd.sessions.values, color }},
        {{ label:'Users',    values: (gd.users||{{values:[]}}).values, color:'#94a3b8' }},
      ]);
    // Installs
    const instId = `chart-${{tabId}}-installs`;
    if (gd.installs && gd.installs.dates.length) {{
      const el = document.getElementById(instId);
      if (el) el.style.display = '';
      makeLineChart(instId, gd.installs.dates, [
        {{ label:'Downloads', values: gd.installs.values, color:'#f59e0b' }}
      ]);
    }}
    // MRR + Subscribers (dual axis via two datasets)
    const revId = `chart-${{tabId}}-revenue`;
    if (gd.mrr && gd.mrr.dates.length) {{
      const el = document.getElementById(revId);
      if (el) el.style.display = '';
      makeLineChart(revId, gd.mrr.dates, [
        {{ label:'MRR ($)',       values: gd.mrr.values,         color:'#10b981' }},
        {{ label:'Subscribers',   values: (gd.subscribers||{{values:[]}}).values, color:'#8b5cf6' }},
      ]);
    }}
    // Unhide sessions canvas if we have data
    if (gd.sessions && gd.sessions.dates.length) {{
      const el = document.getElementById(`chart-${{tabId}}-sessions`);
      if (el) el.style.display = '';
    }}
  }}
}}

function showTab(tabId) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.add('hidden'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById(tabId);
  const btn  = document.getElementById('btn-' + tabId);
  if (pane) pane.classList.remove('hidden');
  if (btn)  btn.classList.add('active');
}}

function toggleRaw(el) {{
  const pre = el.nextElementSibling;
  if (pre.style.display === 'none') {{
    pre.style.display = 'block';
    el.textContent = '\u25bc Hide raw response';
  }} else {{
    pre.style.display = 'none';
    el.textContent = '\u25ba Show raw response';
  }}
}}

window.addEventListener('DOMContentLoaded', initCharts);
</script>
</body>
</html>"""
    return html

# ── PDF Layout ──────────────────────────────────────────────────────────────────

def render_pdf_html(groups, rc_by_group, asc_by_group, total_projects, start_dt, end_dt):
    """Compact single-page A4-landscape PDF layout using tables."""
    date_range = f"{start_dt.strftime('%b %d')} – {end_dt.strftime('%b %d, %Y')}"
    generated  = datetime.now().strftime("%b %d, %Y at %I:%M %p")

    all_results    = [p for g in groups.values() for p in g]
    total_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in all_results)
    total_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in all_results)
    ok_groups      = sum(
        1 for g in groups.values()
        if any(extract_clarity_metrics(r["raw"]).get("sessions", 0) > 0 for r in g)
    )
    total_mrr = total_rev = 0.0
    for rc_data in rc_by_group.values():
        m = rc_data.get("_metrics") or extract_revenuecat_metrics(rc_data)
        total_mrr += m.get("mrr",     0)
        total_rev += m.get("revenue", 0)

    rc_sum_html = ""
    if rc_by_group:
        rc_sum_html = (
            f'<td class="si"><div class="sv">${total_mrr:,.0f}</div><div class="sl">Total MRR</div></td>'
            f'<td class="si"><div class="sv">${total_rev:,.0f}</div><div class="sl">Total Revenue</div></td>'
        )

    # Sort groups
    GROUP_ORDER = ["Shift","Today's Front Pages","Quiet Collection","P3","Self Speak","Footsteps with Jesus"]
    def gkey(item): return (GROUP_ORDER.index(item[0]) if item[0] in GROUP_ORDER else len(GROUP_ORDER), item[0])
    sorted_groups = sorted(groups.items(), key=gkey)

    PCOL = {"iOS": "#007aff", "Android": "#3ddc84", "Website": "#f59e0b"}
    PORD = {"iOS": 0, "Android": 1, "Website": 2}

    DEVICE_ICONS_PDF = {"Mobile": "📱", "Tablet": "⬛", "PC": "🖥"}

    def make_card(group_name, members):
        plat_rows = ""
        for r in sorted(members, key=lambda x: PORD.get(x["platform"], 99)):
            pl = r["platform"]
            m  = extract_clarity_metrics(r["raw"])
            s  = int(m.get("sessions", 0))
            u  = int(m.get("users",    0))
            sp = m.get("screensPerSession", 0)
            en = m.get("engagementSec",     0)
            en_str = f"{en/60:.1f}m" if en >= 60 else f"{int(en)}s"
            c  = PCOL.get(pl, "#94a3b8")

            # Device breakdown pills
            devices = m.get("devices", {})
            device_pills = ""
            for dev in ["Mobile", "Tablet", "PC"]:
                if dev in devices:
                    icon = DEVICE_ICONS_PDF.get(dev, "•")
                    pct  = devices[dev]["pct"]
                    device_pills += f'<span class="dpill">{icon} {pct}%</span>'
            device_row = (
                f'<tr><td colspan="5" class="drow">{device_pills}</td></tr>'
                if device_pills else ""
            )

            plat_rows += (
                f'<tr>'
                f'<td class="pn" style="color:{c};border-left:3px solid {c}">{pl}</td>'
                f'<td class="st">{s:,}</td><td class="st">{u:,}</td>'
                f'<td class="st">{sp:.1f}</td><td class="st">{en_str}</td>'
                f'</tr>'
                f'{device_row}'
            )
        rc_html = ""
        if group_name in rc_by_group:
            rc_data = rc_by_group[group_name]
            m = rc_data.get("_metrics") or extract_revenuecat_metrics(rc_data)
            mrr  = m.get("mrr",         0)
            rev  = m.get("revenue",     0)
            subs = int(m.get("subscribers", 0))
            if mrr > 0 or rev > 0 or subs > 0:
                rc_html = (
                    f'<div class="rc">'
                    f'💰 MRR <b>${mrr:,.0f}</b> &nbsp;·&nbsp; Revenue <b>${rev:,.0f}</b>'
                    f' &nbsp;·&nbsp; {subs:,} subs'
                    f'</div>'
                )
        asc_html = ""
        if group_name in asc_by_group:
            asc_data = asc_by_group[group_name]
            if asc_data.get("pending"):
                asc_html = '<div class="rc">🍎 App Store data initializing…</div>'
            elif asc_data.get("installs") is not None:
                asc_html = (
                    f'<div class="rc">🍎 iOS Downloads <b>{int(asc_data["installs"]):,}</b></div>'
                )
        return (
            f'<td class="card">'
            f'<div class="ct">{group_name}</div>'
            f'<table class="mt"><tr class="mh">'
            f'<th></th><th>SESSIONS</th><th>USERS</th><th>SCREENS</th><th>ENG</th>'
            f'</tr>{plat_rows}</table>'
            f'{rc_html}'
            f'{asc_html}'
            f'</td>'
        )

    # Build rows of 3 cards
    card_rows_html = ""
    for i in range(0, len(sorted_groups), 3):
        chunk = sorted_groups[i:i+3]
        cells = "".join(make_card(g, m) for g, m in chunk)
        # Pad to 3 if needed
        while len(chunk) < 3:
            cells += '<td class="card" style="border:none;background:transparent"></td>'
            chunk.append(None)
        card_rows_html += f'<tr class="cr">{cells}</tr>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><style>
@page {{ size: A4 landscape; margin: 7mm; }}
* {{ box-sizing: border-box; margin: 0; padding: 0;
     font-family: -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif; }}
body {{ background: white; font-size: 9pt; color: #1e293b; }}

.hdr {{ background: #1e293b; color: white; padding: 4mm 6mm;
        display: flex; justify-content: space-between; align-items: center;
        -webkit-print-color-adjust: exact; print-color-adjust: exact; border-radius: 5px 5px 0 0; }}
.hdr h1 {{ font-size: 12pt; font-weight: 700; }}
.hdr .meta {{ font-size: 7pt; opacity: 0.65; margin-top: 1.5mm; }}

.sum {{ background: #3b82f6; color: white; width: 100%;
        border-collapse: collapse;
        -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
.si {{ padding: 3mm 5mm; text-align: center;
      border-right: 1px solid rgba(255,255,255,0.18); }}
.si:last-child {{ border-right: none; }}
.sv {{ font-size: 14pt; font-weight: 700; line-height: 1; }}
.sl {{ font-size: 6pt; opacity: 0.8; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 1mm; }}

.grid {{ width: 100%; border-collapse: separate; border-spacing: 3mm; margin-top: 0; }}
.cr {{ vertical-align: top; }}
.card {{ width: 33.3%; background: white; border: 1px solid #e2e8f0;
         border-radius: 6px; padding: 3mm 4mm;
         -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
.ct {{ font-size: 10.5pt; font-weight: 700; color: #1e293b;
       margin-bottom: 2.5mm; padding-bottom: 2mm;
       border-bottom: 1px solid #f1f5f9; }}

.mt {{ width: 100%; border-collapse: collapse; }}
.mh th {{ font-size: 5.5pt; color: #94a3b8; text-transform: uppercase;
          letter-spacing: 0.05em; padding: 0 2mm 1.5mm 2mm; text-align: right; }}
.mh th:first-child {{ text-align: left; padding-left: 4mm; }}
.pn {{ font-size: 7.5pt; font-weight: 700; padding: 1.5mm 2mm 1.5mm 4mm;
       white-space: nowrap; }}
.st {{ font-size: 9pt; font-weight: 600; text-align: right; padding: 1.5mm 2mm; }}
.mt tr:not(.mh) {{ border-top: 1px solid #f8fafc; }}

.drow {{ padding: 0.5mm 2mm 1.5mm 4mm; }}
.dpill {{ display: inline-block; font-size: 6.5pt; color: #475569;
          background: #f1f5f9; border-radius: 3px;
          padding: 0.5mm 1.5mm; margin-right: 1.5mm; }}

.rc {{ font-size: 7pt; color: #16a34a; margin-top: 2.5mm; padding-top: 2mm;
       border-top: 1px solid #dcfce7; }}
.rc b {{ font-weight: 700; }}

.foot {{ text-align: center; color: #94a3b8; font-size: 6.5pt; padding: 2mm 0 0 0; }}
</style></head><body>

<div class="hdr">
  <div>
    <h1>📊 Microsoft Clarity — All Projects</h1>
    <div class="meta">Last 30 days &nbsp;·&nbsp; {date_range} &nbsp;·&nbsp; Generated {generated}</div>
  </div>
</div>

<table class="sum"><tr>
  <td class="si"><div class="sv">{len(groups)}</div><div class="sl">Apps / Sites</div></td>
  <td class="si"><div class="sv">{total_projects}</div><div class="sl">Platforms</div></td>
  <td class="si"><div class="sv">{int(total_sessions):,}</div><div class="sl">Total Sessions</div></td>
  <td class="si"><div class="sv">{int(total_users):,}</div><div class="sl">Total Users</div></td>
  <td class="si"><div class="sv">{ok_groups}/{len(groups)}</div><div class="sl">With Data</div></td>
  {rc_sum_html}
</tr></table>

<table class="grid">{card_rows_html}</table>
<div class="foot">Clarity Dashboard &nbsp;·&nbsp; Refresh: python3 clarity_dashboard.py</div>
</body></html>"""

# ── Main ────────────────────────────────────────────────────────────────────────

def _env_key_name(group):
    """Derive an env-var-safe name from a group string, e.g. "Today's Front Pages" → ASC_KEY_TODAYS_FRONT_PAGES"""
    clean = group.upper().replace("'", "").replace("\u2019", "")  # remove apostrophes first
    slug  = re.sub(r"[^A-Z0-9]+", "_", clean).strip("_")
    return f"ASC_KEY_{slug}"

def main():
    # ── Load config from env var (GitHub Actions) or local file (dev) ───────────
    config_json_env = os.environ.get("CONFIG_JSON", "").strip()
    if config_json_env:
        print("  [config] Loading from CONFIG_JSON environment variable")
        try:
            config = json.loads(config_json_env)
        except json.JSONDecodeError as e:
            print(f"❌  CONFIG_JSON is not valid JSON: {e}")
            sys.exit(1)
    elif os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            config = json.load(f)
    else:
        print(f"❌  No config found. Set CONFIG_JSON env var or create '{CONFIG_FILE}'.")
        sys.exit(1)

    projects = config.get("projects", [])
    if not projects:
        print("❌  No projects listed in config.json")
        sys.exit(1)

    end_dt   = datetime.now() - timedelta(days=1)   # end yesterday (exclude partial today)
    start_dt = end_dt - timedelta(days=DAYS_BACK)

    print(f"\n📊 Clarity Dashboard Generator")
    print(f"   Period  : {start_dt.strftime('%b %d')} – {end_dt.strftime('%b %d, %Y')}")
    print(f"   Projects: {len(projects)}\n")

    # Load accumulated history (builds trend charts over time)
    history = _history_load()
    print(f"   History : {sum(len(v) for v in history.get('clarity',{}).values())} clarity snapshots, "
          f"{sum(len(v) for v in history.get('revenuecat',{}).values())} RC snapshots\n")

    # Note: Clarity's API returns rolling totals regardless of date range — backfill
    # doesn't produce useful daily data. History grows one real snapshot per weekly run.

    groups = defaultdict(list)

    for proj in projects:
        name     = proj.get("name", "Unnamed")
        token    = proj.get("api_token", "").strip()
        group    = proj.get("group", name)
        platform = proj.get("platform", "Unknown")

        print(f"  → {name} ...", end=" ", flush=True)

        if not token or token == "PASTE_YOUR_TOKEN_HERE":
            print("⏭  skipped (no token)")
            continue

        raw  = fetch_project(name, token, start_dt, end_dt)
        data = raw if isinstance(raw, dict) else {}

        print("✅" if raw else "❌ no data")

        groups[group].append({
            "name":     name,
            "platform": platform,
            "data":     data,
            "raw":      raw,
        })

    if not groups:
        print("\n❌  No data fetched. Double-check your tokens in config.json")
        sys.exit(1)

    # ── Snapshot Clarity aggregates into history (for trend charts) ─────────────
    print("📅 Saving Clarity snapshots to history...")
    for gname, members in groups.items():
        g_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in members)
        g_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in members)
        if g_sessions > 0 or g_users > 0:
            _history_append_clarity(history, gname, g_sessions, g_users)

    # ── RevenueCat ──────────────────────────────────────────────────────────────
    rc_by_group = {}
    rc_config   = config.get("revenuecat", {})
    rc_apps     = rc_config.get("apps", [])

    if rc_apps:
        print(f"\n💰 Fetching RevenueCat data ({len(rc_apps)} apps)...\n")
        for app in rc_apps:
            group_name = app.get("group", "")
            rc_key     = app.get("api_key", "").strip()
            print(f"  → {group_name} ...", end=" ", flush=True)
            if not rc_key or rc_key.startswith("PASTE_"):
                print("⏭  skipped (no API key)")
                continue
            rc_data = fetch_revenuecat(rc_key, start_dt, end_dt)
            print("✅" if rc_data else "❌ no data")
            if rc_data:
                rc_by_group[group_name] = rc_data
                # Snapshot RC metrics into history for trend charts
                rc_m = rc_data.get("_metrics") or extract_revenuecat_metrics(rc_data)
                if rc_m.get("mrr") or rc_m.get("subscribers"):
                    _history_append_revenuecat(history, group_name, rc_m)
    else:
        print("\n💰 RevenueCat: skipped (no apps configured)")

    # ── App Store Connect ────────────────────────────────────────────────────────
    asc_by_group = {}
    asc_config   = config.get("appstore_connect", {})
    asc_apps     = asc_config.get("apps", [])

    if asc_apps:
        print(f"\n🍎 Fetching App Store Connect data ({len(asc_apps)} apps)...\n")
        for app in asc_apps:
            group_name = app.get("group", "")
            apple_id   = str(app.get("apple_id", "")).strip()
            key_id     = app.get("key_id", "").strip()
            issuer_id  = app.get("issuer_id", "").strip()
            key_file   = app.get("key_file", "").strip()
            print(f"  → {group_name} ...", end=" ", flush=True)
            if not all([apple_id, key_id, issuer_id, key_file]):
                print("⏭  skipped (incomplete config)")
                continue
            if not os.path.exists(key_file):
                # Try loading key content from env var (e.g. ASC_KEY_SHIFT, ASC_KEY_P3)
                env_var_name = _env_key_name(group_name)
                key_content  = os.environ.get(env_var_name, "").strip()
                if key_content:
                    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".p8", delete=False)
                    tmp.write(key_content)
                    tmp.close()
                    key_file = tmp.name
                    print(f"\n    [ASC] Using {env_var_name} env var for key", end=" ")
                else:
                    print(f"⏭  skipped (.p8 not found and {env_var_name} not set)")
                    continue
            asc_data = fetch_appstore(apple_id, key_id, issuer_id, key_file)
            if asc_data and asc_data.get("pending"):
                print("⏳ pending (report initializing)")
            elif asc_data and asc_data.get("installs") is not None:
                print(f"✅ {int(asc_data['installs']):,} installs")
            else:
                print("❌ no data")
            if asc_data:
                asc_by_group[group_name] = asc_data
    else:
        print("\n🍎 App Store Connect: skipped (no apps configured)")

    # Save accumulated history so next run builds on it
    _history_save(history)
    print(f"📅 History saved → {HISTORY_FILE}")

    html = render_html(groups, rc_by_group, asc_by_group, len(projects), start_dt, end_dt, history=history)

    with open(OUTPUT_FILE, "w") as f:
        f.write(html)

    print(f"\n✅  HTML saved → {OUTPUT_FILE}")

    # Generate PDF from compact single-page layout (not the web HTML)
    pdf_html = render_pdf_html(groups, rc_by_group, asc_by_group, len(projects), start_dt, end_dt)

    if HAS_WEASYPRINT == "python":
        print("📄  Generating PDF...", end=" ", flush=True)
        try:
            weasyprint.HTML(string=pdf_html).write_pdf(OUTPUT_PDF)
            print(f"✅  PDF saved → {OUTPUT_PDF}")
        except Exception as e:
            print(f"⚠️  PDF generation failed: {e}")
    elif HAS_WEASYPRINT == "cli":
        print("📄  Generating PDF (CLI)...", end=" ", flush=True)
        try:
            # Write to a hidden system temp file — won't appear in your folder
            with tempfile.NamedTemporaryFile(mode="w", suffix=".html",
                                             delete=False, prefix=".tmp_clarity_") as tf:
                tf.write(pdf_html)
                tmp_path = tf.name
            abs_pdf = os.path.abspath(OUTPUT_PDF)
            r = subprocess.run(
                ["weasyprint", tmp_path, abs_pdf],
                capture_output=True, text=True
            )
            os.unlink(tmp_path)  # clean up temp file immediately
            if r.returncode == 0:
                print(f"✅  PDF saved → {OUTPUT_PDF}")
            else:
                print(f"⚠️  weasyprint CLI error: {r.stderr[:300]}")
        except Exception as e:
            print(f"⚠️  PDF generation failed: {e}")
    else:
        print("⚠️  weasyprint not found — PDF skipped.")
        print("   Install it with: pip3 install weasyprint  OR  brew install weasyprint")

    print()

if __name__ == "__main__":
    main()
