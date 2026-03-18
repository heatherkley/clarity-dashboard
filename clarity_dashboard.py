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

CONFIG_FILE  = "config.json"
OUTPUT_FILE  = "clarity_dashboard.html"
OUTPUT_PDF   = "clarity_dashboard.pdf"
DAYS_BACK    = 30

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
            if result.get("pending"):
                # Newly created — just wait
                print(f"    [ASC] No reports yet (new request — check back in 24–48h)")
            else:
                # Old request but no reports — delete and recreate
                print(f"    [ASC] No reports under existing request — resetting...")
                requests.delete(f"{ASC_BASE}/analyticsReportRequests/{ongoing_id}",
                                headers=hdrs, timeout=20)
                cache.pop(cache_key, None)
                new_id, _ = _asc_create_request(apple_id, hdrs)
                if new_id:
                    cache[cache_key] = new_id
                _asc_cache_save(cache)
                print(f"    [ASC] Fresh request created — check back in 24–48h")
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
        for row in reader:
            val = row.get("Installations") or row.get("First Time Downloads") or 0
            try:
                total_installs += int(float(str(val).replace(",", "") or 0))
            except (ValueError, TypeError):
                pass
        result["installs"] = total_installs
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
            'Data initializing — check back tomorrow</div></div>'
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

def render_html(groups, rc_by_group, asc_by_group, total_projects, start_dt, end_dt):
    date_range = f"{start_dt.strftime('%b %d')} – {end_dt.strftime('%b %d, %Y')}"
    generated  = datetime.now().strftime("%b %d, %Y at %I:%M %p")

    # Summary totals across all platforms
    all_results    = [p for g in groups.values() for p in g]
    total_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in all_results)
    total_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in all_results)
    ok_groups      = sum(
        1 for g in groups.values()
        if any(extract_clarity_metrics(r["raw"]).get("sessions", 0) > 0 for r in g)
    )

    # Revenue totals across all RevenueCat apps
    total_mrr     = 0.0
    total_revenue = 0.0
    for rc_data in rc_by_group.values():
        if "_metrics" in rc_data:
            m = rc_data["_metrics"]
        else:
            m = extract_revenuecat_metrics(rc_data)
        total_mrr     += m.get("mrr",     0)
        total_revenue += m.get("revenue", 0)
    mrr_str = "${:,.0f}".format(total_mrr)
    rev_str = "${:,.0f}".format(total_revenue)
    rc_summary_html = ""
    if rc_by_group:
        rc_summary_html = f"""
  <div class="summary-item">
    <div class="summary-value">{mrr_str}</div>
    <div class="summary-label">Total MRR</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{rev_str}</div>
    <div class="summary-label">Total Revenue</div>
  </div>"""

    # Build group cards — custom order, remaining groups alphabetically at the end
    GROUP_ORDER = [
        "Shift",
        "Today's Front Pages",
        "Quiet Collection",
        "P3",
        "Self Speak",
        "Footsteps with Jesus",
    ]
    def group_sort_key(item):
        name = item[0]
        return (GROUP_ORDER.index(name) if name in GROUP_ORDER else len(GROUP_ORDER), name)

    cards_html = ""
    for group_name, members in sorted(groups.items(), key=group_sort_key):
        group_sessions = sum(extract_clarity_metrics(r["raw"]).get("sessions", 0) for r in members)
        group_users    = sum(extract_clarity_metrics(r["raw"]).get("users",    0) for r in members)

        platform_rows = ""
        PLATFORM_ORDER = {"iOS": 0, "Android": 1, "Website": 2}
        for r in sorted(members, key=lambda x: PLATFORM_ORDER.get(x["platform"], 99)):
            platform_rows += platform_row(r["platform"], r["data"], r["raw"])

        combined_block = ""
        if len(members) > 1:
            combined_block = f"""
        <div class="combined-total">
          <span class="combined-label">Combined</span>
          <span class="combined-stat">{int(group_sessions):,} sessions</span>
          <span class="combined-sep">·</span>
          <span class="combined-stat">{int(group_users):,} users</span>
        </div>"""

        rc_html  = revenuecat_block(rc_by_group.get(group_name))
        asc_html = appstore_block(asc_by_group.get(group_name))

        cards_html += f"""
      <div class="card">
        <div class="card-header">
          <span class="project-name">{group_name}</span>
          <span class="platform-count">{len(members)} platform{"s" if len(members) != 1 else ""}</span>
        </div>
        {combined_block}
        {platform_rows}
        {rc_html}
        {asc_html}
      </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Clarity Dashboard — {date_range}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f1f5f9;
      color: #1e293b;
      min-height: 100vh;
    }}
    header {{
      background: #1e293b;
      color: white;
      padding: 24px 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }}
    header h1 {{ font-size: 1.4rem; font-weight: 600; }}
    .meta {{ font-size: 0.85rem; opacity: 0.7; }}
    .summary {{
      background: #3b82f6;
      color: white;
      display: flex;
      flex-wrap: wrap;
    }}
    .summary-item {{
      flex: 1;
      min-width: 140px;
      padding: 20px 28px;
      border-right: 1px solid rgba(255,255,255,0.15);
    }}
    .summary-item:last-child {{ border-right: none; }}
    .summary-value {{ font-size: 2rem; font-weight: 700; }}
    .summary-label {{ font-size: 0.75rem; opacity: 0.8; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.05em; }}
    .legend {{
      background: #1e293b;
      padding: 10px 32px;
      display: flex;
      gap: 20px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .legend-item {{ display: flex; align-items: center; gap: 6px; font-size: 0.78rem; color: #94a3b8; }}
    .legend-dot {{ width: 10px; height: 10px; border-radius: 2px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
      gap: 20px;
      padding: 28px 32px;
      max-width: 1600px;
      margin: 0 auto;
    }}
    .card {{
      background: white;
      border-radius: 12px;
      padding: 20px 24px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }}
    .card-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      padding-bottom: 12px;
      border-bottom: 1px solid #f1f5f9;
    }}
    .project-name {{ font-weight: 700; font-size: 1rem; color: #1e293b; }}
    .platform-count {{ font-size: 0.75rem; color: #94a3b8; }}
    .combined-total {{
      display: flex;
      align-items: center;
      gap: 8px;
      background: #f8fafc;
      border-radius: 8px;
      padding: 8px 12px;
      margin-bottom: 12px;
      font-size: 0.82rem;
    }}
    .combined-label {{ font-weight: 600; color: #475569; }}
    .combined-stat {{ color: #1e293b; font-weight: 500; }}
    .combined-sep {{ color: #cbd5e1; }}
    .platform-row {{
      border-radius: 8px;
      padding: 12px 12px 12px 14px;
      margin-bottom: 10px;
      background: #fafafa;
    }}
    .platform-row:last-child {{ margin-bottom: 0; }}
    .platform-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 10px;
    }}
    .badge {{
      color: white;
      font-size: 0.7rem;
      font-weight: 600;
      padding: 3px 8px;
      border-radius: 4px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .status-dot {{
      width: 8px; height: 8px;
      border-radius: 50%;
    }}
    .metrics {{
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }}
    .metric {{
      flex: 1;
      min-width: 76px;
      background: white;
      border-radius: 6px;
      padding: 8px 10px;
      text-align: center;
      border: 1px solid #f1f5f9;
    }}
    .metric-value {{ font-size: 1.1rem; font-weight: 700; color: #1e293b; }}
    .metric-label {{ font-size: 0.65rem; color: #64748b; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap; overflow: visible; }}
    .device-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid #f1f5f9;
    }}
    .device-pill {{
      font-size: 0.75rem;
      color: #475569;
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 20px;
      padding: 4px 10px;
    }}
    .device-pill strong {{ color: #1e293b; }}
    .device-pct {{ color: #94a3b8; font-size: 0.7rem; }}
    .rc-block {{
      margin-top: 12px;
      padding: 12px 14px;
      background: #f0fdf4;
      border: 1px solid #bbf7d0;
      border-radius: 8px;
    }}
    .rc-block.rc-debug {{ background: #fefce8; border-color: #fde68a; }}
    .rc-title {{ font-size: 0.75rem; font-weight: 600; color: #166534; display: block; margin-bottom: 8px; }}
    .rc-block.rc-debug .rc-title {{ color: #92400e; }}
    .rc-metrics {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .rc-metric {{
      flex: 1;
      min-width: 72px;
      background: white;
      border-radius: 6px;
      padding: 7px 10px;
      text-align: center;
      border: 1px solid #dcfce7;
    }}
    .rc-value {{ font-size: 1rem; font-weight: 700; color: #166534; }}
    .rc-label {{ font-size: 0.65rem; color: #4ade80; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.04em; }}
    .raw-toggle {{
      margin-top: 10px;
      font-size: 0.72rem;
      color: #94a3b8;
      cursor: pointer;
      user-select: none;
    }}
    .raw-toggle:hover {{ color: #3b82f6; }}
    .raw-data {{
      margin-top: 6px;
      background: #f1f5f9;
      border-radius: 6px;
      padding: 10px;
      font-size: 0.68rem;
      overflow-x: auto;
      color: #475569;
      white-space: pre-wrap;
      word-break: break-all;
    }}
    footer {{
      text-align: center;
      padding: 24px;
      font-size: 0.8rem;
      color: #94a3b8;
    }}
    code {{ background: #f1f5f9; padding: 2px 6px; border-radius: 4px; }}
  @media print, (format: pdf) {{
    @page {{ size: A4 landscape; margin: 12mm; }}
    body {{ background: #f1f5f9; }}
    header {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .summary {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .legend {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .card {{ break-inside: avoid; }}
    .raw-toggle, .raw-data {{ display: none !important; }}
    .grid {{ grid-template-columns: repeat(3, 1fr); padding: 16px; gap: 14px; }}
  }}
  </style>
</head>
<body>

<header>
  <div>
    <h1>📊 Microsoft Clarity — All Projects</h1>
    <div class="meta">Last 30 days &nbsp;·&nbsp; {date_range}</div>
  </div>
  <div class="meta">Generated {generated}</div>
</header>

<div class="summary">
  <div class="summary-item">
    <div class="summary-value">{len(groups)}</div>
    <div class="summary-label">Apps / Sites</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{total_projects}</div>
    <div class="summary-label">Total Platforms</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{int(total_sessions):,}</div>
    <div class="summary-label">Total Sessions</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{int(total_users):,}</div>
    <div class="summary-label">Total Users</div>
  </div>
  <div class="summary-item">
    <div class="summary-value">{ok_groups}/{len(groups)}</div>
    <div class="summary-label">Apps with Data</div>
  </div>
  {rc_summary_html}
</div>

<div class="legend">
  <span style="color:#94a3b8;font-size:0.75rem;">PLATFORMS:</span>
  <div class="legend-item"><div class="legend-dot" style="background:#007aff"></div> iOS</div>
  <div class="legend-item"><div class="legend-dot" style="background:#3ddc84"></div> Android</div>
  <div class="legend-item"><div class="legend-dot" style="background:#f59e0b"></div> Website</div>
</div>

<div class="grid">
{cards_html}
</div>

<footer>Clarity Dashboard · Refresh by running <code>python3 clarity_dashboard.py</code> again</footer>

<script>
function toggleRaw(el) {{
  const pre = el.nextElementSibling;
  if (pre.style.display === 'none') {{
    pre.style.display = 'block';
    el.textContent = '▼ Hide raw response';
  }} else {{
    pre.style.display = 'none';
    el.textContent = '▶ Show raw response';
  }}
}}
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
    import re
    clean = group.upper().replace("'", "").replace("'", "")  # remove apostrophes first
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

    html = render_html(groups, rc_by_group, asc_by_group, len(projects), start_dt, end_dt)

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
