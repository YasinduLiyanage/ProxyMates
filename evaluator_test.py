"""
ProxyMaze'26 — Evaluator-Style Black Box Test
==============================================

Simulates exactly how Torch Labs will evaluate your service.

SETUP (3 terminals):
  Terminal 1: python webhook_receiver.py          (port 9000)
  Terminal 2: uvicorn main:app --port 8000        (port 8000)
  Terminal 3: python evaluator_test.py            (this file)
"""

import time
import re
import sys
import httpx

API = "http://localhost:8000"
WEBHOOK = "http://localhost:9000"
PASS = 0
FAIL = 0
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

client = httpx.Client(base_url=API, timeout=30.0)
wh_client = httpx.Client(base_url=WEBHOOK, timeout=10.0)


def log(name: str, ok: bool, detail: str = ""):
    global PASS, FAIL
    if ok:
        PASS += 1
    else:
        FAIL += 1
    tag = "PASS" if ok else "FAIL"
    line = f"  [{tag}] {name}"
    if not ok and detail:
        line += f"  -->  {detail}"
    print(line)


def eq(name, actual, expected):
    log(name, actual == expected, f"expected {expected!r}, got {actual!r}")


def ok(name, cond, detail=""):
    log(name, bool(cond), detail)


def section(title):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")


def clear_webhook_receiver():
    wh_client.delete("/received")


def get_webhooks():
    return wh_client.get("/received").json()


def wait_for_check(seconds=5):
    print(f"  ... waiting {seconds}s for background check cycle ...")
    time.sleep(seconds)


# ===========================================================================
# PRE-FLIGHT: Connectivity
# ===========================================================================
def preflight():
    section("Pre-flight checks")
    try:
        r = client.get("/health")
        ok("ProxyMaze server reachable", r.status_code == 200)
    except Exception as e:
        print(f"  FATAL: Cannot reach ProxyMaze at {API}: {e}")
        print(f"  Start it: uvicorn main:app --port 8000")
        sys.exit(1)

    try:
        r = wh_client.get("/received")
        ok("Webhook receiver reachable", r.status_code == 200)
    except Exception as e:
        print(f"  FATAL: Cannot reach webhook receiver at {WEBHOOK}: {e}")
        print(f"  Start it: python webhook_receiver.py")
        sys.exit(1)

    clear_webhook_receiver()


# ===========================================================================
# CATEGORY 1: Service bootstrap and configuration (10 pts)
# ===========================================================================
def test_bootstrap_and_config():
    section("Category 1: Service Bootstrap & Configuration (10 pts)")

    # Ch1: GET /health
    r = client.get("/health")
    eq("GET /health -> 200", r.status_code, 200)
    eq("body has status:ok", r.json().get("status"), "ok")

    # Ch2: POST /config
    r = client.post("/config", json={"check_interval_seconds": 3, "request_timeout_ms": 3000})
    eq("POST /config -> 200", r.status_code, 200)
    body = r.json()
    eq("check_interval_seconds accepted", body["check_interval_seconds"], 3)
    eq("request_timeout_ms accepted", body["request_timeout_ms"], 3000)

    # Ch3: GET /config matches
    r = client.get("/config")
    eq("GET /config -> 200", r.status_code, 200)
    eq("GET /config matches POST", r.json(), body)

    # Unknown fields silently accepted
    r = client.post("/config", json={"check_interval_seconds": 3, "unknown_stuff": 42})
    eq("unknown fields accepted", r.status_code, 200)

    # Malformed JSON
    r = client.post("/config", content=b"{bad json", headers={"Content-Type": "application/json"})
    eq("malformed JSON -> 400", r.status_code, 400)


# ===========================================================================
# CATEGORY 2: Proxy pool ingestion and background monitoring (45 pts)
# ===========================================================================
def test_pool_ingestion_and_monitoring():
    section("Category 2: Pool Ingestion & Background Monitoring (45 pts)")

    # Clean slate
    client.delete("/proxies")

    # Ch4: POST /proxies
    proxy_urls = [
        "https://httpbin.org/status/200",       # -> up   (id: 200)
        "https://httpbin.org/get",               # -> up   (id: get)
        "https://httpbin.org/ip",                # -> up   (id: ip)
        "https://httpbin.org/status/500",        # -> down (id: 500)
        "https://httpbin.org/status/503",        # -> down (id: 503)
    ]
    r = client.post("/proxies", json={"proxies": proxy_urls})
    eq("POST /proxies -> 201", r.status_code, 201)
    body = r.json()
    eq("accepted count", body["accepted"], 5)
    eq("proxies array length", len(body["proxies"]), 5)

    # Verify IDs are deterministic (last path segment)
    ids = [p["id"] for p in body["proxies"]]
    eq("ID extraction: 200", ids[0], "200")
    eq("ID extraction: get", ids[1], "get")
    eq("ID extraction: 500", ids[3], "500")

    # All start as pending
    for p in body["proxies"]:
        eq(f"proxy {p['id']} starts pending", p["status"], "pending")

    # Unknown fields in POST /proxies
    r = client.post("/proxies", json={"proxies": [], "some_random_field": True})
    eq("POST /proxies ignores unknown fields", r.status_code, 201)

    # Replace mode
    r = client.post("/proxies", json={"proxies": proxy_urls, "replace": True})
    eq("replace mode -> 201", r.status_code, 201)
    eq("replace mode accepted count", r.json()["accepted"], 5)

    # Ch5: GET /proxies (before check — all pending)
    r = client.get("/proxies")
    eq("GET /proxies -> 200", r.status_code, 200)
    data = r.json()
    eq("total is 5", data["total"], 5)
    ok("failure_rate present", "failure_rate" in data)
    ok("proxies is array", isinstance(data["proxies"], list))
    for p in data["proxies"]:
        ok(f"{p['id']} has required fields",
           all(k in p for k in ["id", "url", "status", "last_checked_at", "consecutive_failures"]))

    # Wait for background check
    wait_for_check(5)

    # After check: verify real probe results
    r = client.get("/proxies")
    data = r.json()
    print(f"  Pool: total={data['total']} up={data['up']} down={data['down']} rate={data['failure_rate']:.2f}")

    ok("proxies are no longer pending",
       all(p["status"] in ("up", "down") for p in data["proxies"]),
       f"statuses: {[p['status'] for p in data['proxies']]}")
    ok("some proxies are up", data["up"] > 0, f"up={data['up']}")
    ok("some proxies are down", data["down"] > 0, f"down={data['down']}")
    ok("failure_rate is float", isinstance(data["failure_rate"], (int, float)))

    # Ch6: GET /proxies/{id} — known proxy
    r = client.get("/proxies/200")
    eq("GET /proxies/200 -> 200", r.status_code, 200)
    d = r.json()
    eq("proxy 200 id", d["id"], "200")
    eq("proxy 200 status", d["status"], "up")
    ok("has total_checks", d["total_checks"] >= 1, f"total_checks={d['total_checks']}")
    ok("has uptime_percentage", "uptime_percentage" in d)
    ok("uptime_percentage type", isinstance(d["uptime_percentage"], (int, float)))
    ok("has history array", isinstance(d["history"], list) and len(d["history"]) >= 1)
    ok("last_checked_at is ISO", ISO_RE.match(d["last_checked_at"] or ""))
    ok("consecutive_failures is int", isinstance(d["consecutive_failures"], int))

    r = client.get("/proxies/500")
    if r.status_code == 200:
        d = r.json()
        eq("proxy 500 status", d["status"], "down")
        ok("proxy 500 consecutive_failures >= 1", d["consecutive_failures"] >= 1)

    # Ch6: 404 for unknown proxy
    r = client.get("/proxies/nonexistent-proxy-xyz")
    eq("unknown proxy -> 404", r.status_code, 404)

    # Ch7: GET /proxies/{id}/history
    r = client.get("/proxies/200/history")
    eq("GET /proxies/200/history -> 200", r.status_code, 200)
    history = r.json()
    ok("history is array", isinstance(history, list))
    ok("history has entries", len(history) >= 1)
    if history:
        ok("entry has checked_at", "checked_at" in history[0])
        ok("entry has status", "status" in history[0])
        ok("checked_at is ISO", ISO_RE.match(history[0]["checked_at"]))

    # Ch7: 404 for unknown proxy history
    r = client.get("/proxies/nonexistent-proxy-xyz/history")
    eq("unknown proxy history -> 404", r.status_code, 404)


# ===========================================================================
# CATEGORY 3: Single failure behavior (30 pts)
# ===========================================================================
def test_single_failure():
    section("Category 3: Single Failure Behavior (30 pts)")

    # Check that a single failed proxy in a pool of 5 (20%) triggers correctly
    # failure_rate = 1/5 = 0.20 which IS >= 0.20 threshold
    client.delete("/proxies")
    # 4 up + 1 down = 20% = exactly at threshold
    r = client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/get",
            "https://httpbin.org/ip",
            "https://httpbin.org/headers",
            "https://httpbin.org/status/500",
        ],
        "replace": True,
    })
    eq("loaded 5 proxies (1 will fail)", r.json()["accepted"], 5)

    wait_for_check(5)

    r = client.get("/proxies")
    data = r.json()
    down_list = [p for p in data["proxies"] if p["status"] == "down"]
    up_list = [p for p in data["proxies"] if p["status"] == "up"]
    print(f"  Pool: up={len(up_list)} down={len(down_list)} rate={data['failure_rate']:.2f}")

    ok("down proxy 500 detected", any(p["id"] == "500" for p in down_list),
       f"down ids: {[p['id'] for p in down_list]}")
    ok("failure_rate computed correctly", abs(data["failure_rate"] - len(down_list)/data["total"]) < 0.01,
       f"rate={data['failure_rate']}")

    # Check the proxy detail for the failed one
    r = client.get("/proxies/500")
    if r.status_code == 200:
        d = r.json()
        eq("failed proxy status is down", d["status"], "down")
        ok("consecutive_failures tracked", d["consecutive_failures"] >= 1)
        ok("uptime_percentage is 0", d["uptime_percentage"] == 0.0)


# ===========================================================================
# CATEGORY 4: Threshold breach alerts & webhook delivery (90 pts)
# ===========================================================================
def test_alerts_and_webhooks():
    section("Category 4: Alerts & Webhook Delivery (90 pts)")

    # Register webhook + integrations BEFORE triggering breach
    clear_webhook_receiver()
    client.delete("/proxies")

    # Wait for monitor to resolve any stale active alert from previous tests
    wait_for_check(5)

    # Register generic webhook
    r = client.post("/webhooks", json={"url": f"{WEBHOOK}/generic"})
    eq("POST /webhooks -> 201", r.status_code, 201)
    wh = r.json()
    ok("webhook_id present", "webhook_id" in wh)
    ok("webhook_id non-empty", len(wh["webhook_id"]) > 0)
    eq("webhook url matches", wh["url"], f"{WEBHOOK}/generic")

    # Register Slack integration
    r = client.post("/integrations", json={
        "type": "slack",
        "webhook_url": f"{WEBHOOK}/slack",
        "username": "TestBot",
        "events": ["alert.fired", "alert.resolved"],
    })
    eq("Slack integration -> 201", r.status_code, 201)
    slack_integ = r.json()
    ok("slack integration has id", "id" in slack_integ)

    # Register Discord integration
    r = client.post("/integrations", json={
        "type": "discord",
        "webhook_url": f"{WEBHOOK}/discord",
        "username": "TestBot",
        "events": ["alert.fired", "alert.resolved"],
    })
    eq("Discord integration -> 201", r.status_code, 201)
    discord_integ = r.json()
    ok("discord integration has id", "id" in discord_integ)

    # Unknown fields in webhook
    r = client.post("/webhooks", json={"url": f"{WEBHOOK}/extra", "extra_field": True})
    eq("webhook unknown fields accepted", r.status_code, 201)

    # Trigger breach: 3 down out of 5 = 60% failure rate
    clear_webhook_receiver()
    r = client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/get",
            "https://httpbin.org/status/500",
            "https://httpbin.org/status/502",
            "https://httpbin.org/status/503",
        ],
        "replace": True,
    })

    wait_for_check(6)

    # Ch9: GET /alerts — should have active alert
    r = client.get("/alerts")
    eq("GET /alerts -> 200", r.status_code, 200)
    alerts_data = r.json()
    ok("alerts is array", isinstance(alerts_data, list))
    active = [a for a in alerts_data if a["status"] == "active"]
    ok("at least one active alert", len(active) >= 1, f"active count: {len(active)}")

    if active:
        alert = active[-1]
        print(f"  Alert: id={alert['alert_id']} rate={alert['failure_rate']:.2f} "
              f"failed={alert['failed_proxies']} total={alert['total_proxies']}")

        # Required fields
        ok("alert_id non-empty", len(alert["alert_id"]) > 0)
        eq("alert status", alert["status"], "active")
        ok("failure_rate >= 0.20", alert["failure_rate"] >= 0.20)
        eq("threshold is 0.2", alert["threshold"], 0.2)
        ok("total_proxies is int", isinstance(alert["total_proxies"], int))
        eq("total_proxies == 5", alert["total_proxies"], 5)
        ok("failed_proxies is int", isinstance(alert["failed_proxies"], int))
        ok("failed_proxies > 0", alert["failed_proxies"] > 0)
        ok("failed_proxy_ids is list", isinstance(alert["failed_proxy_ids"], list))
        ok("failed_proxy_ids non-empty", len(alert["failed_proxy_ids"]) > 0)
        ok("fired_at is ISO", ISO_RE.match(alert["fired_at"]))
        eq("resolved_at is null", alert["resolved_at"], None)
        ok("message non-empty", len(alert.get("message", "")) > 0)

        # CONSISTENCY CHECK: GET /proxies vs GET /alerts
        proxies_data = client.get("/proxies").json()
        down_ids_proxies = sorted([p["id"] for p in proxies_data["proxies"] if p["status"] == "down"])
        down_ids_alert = sorted(alert["failed_proxy_ids"])
        eq("CONSISTENCY: failed_proxy_ids match GET /proxies", down_ids_alert, down_ids_proxies)
        eq("CONSISTENCY: failed_proxies count", alert["failed_proxies"], len(down_ids_proxies))

    # Check webhook payloads received
    time.sleep(2)  # give deliveries a moment
    webhooks_received = get_webhooks()
    generic_fired = [w for w in webhooks_received if w["path"] == "/generic" and w["body"].get("event") == "alert.fired"]
    ok("generic webhook received alert.fired", len(generic_fired) >= 1,
       f"received {len(generic_fired)} generic fired events")

    if generic_fired:
        payload = generic_fired[0]["body"]
        ok("payload has event", payload.get("event") == "alert.fired")
        ok("payload has alert_id", len(payload.get("alert_id", "")) > 0)
        ok("payload has fired_at", ISO_RE.match(payload.get("fired_at", "")))
        ok("payload has failure_rate", isinstance(payload.get("failure_rate"), (int, float)))
        ok("payload has total_proxies", isinstance(payload.get("total_proxies"), int))
        ok("payload has failed_proxies", isinstance(payload.get("failed_proxies"), int))
        ok("payload has failed_proxy_ids", isinstance(payload.get("failed_proxy_ids"), list))
        eq("payload threshold", payload.get("threshold"), 0.2)
        ok("payload has message", len(payload.get("message", "")) > 0)

    # Exactly-once: no duplicate deliveries to same receiver
    generic_all = [w for w in webhooks_received if w["path"] == "/generic" and w["body"].get("event") == "alert.fired"]
    eq("exactly-once: 1 generic fired delivery", len(generic_all), 1)

    return alert if active else None


# ===========================================================================
# CATEGORY 4b: Slack & Discord payload verification (+20 bonus pts)
# ===========================================================================
def test_slack_discord_payloads():
    section("Bonus: Slack & Discord Payload Verification (+20 pts)")

    webhooks_received = get_webhooks()

    # SLACK
    slack_events = [w for w in webhooks_received if w["path"] == "/slack"]
    ok("Slack webhook received", len(slack_events) >= 1, f"got {len(slack_events)}")

    if slack_events:
        s = slack_events[0]["body"]
        ok("slack: has username", isinstance(s.get("username"), str) and len(s["username"]) > 0)
        ok("slack: has text", isinstance(s.get("text"), str) and len(s["text"]) > 0)
        ok("slack: has attachments", isinstance(s.get("attachments"), list) and len(s["attachments"]) > 0)

        if s.get("attachments"):
            att = s["attachments"][0]
            ok("slack: color is hex #RRGGBB", isinstance(att.get("color"), str) and re.match(r"^#[0-9A-Fa-f]{6}$", att["color"]))
            ok("slack: footer non-empty", isinstance(att.get("footer"), str) and len(att["footer"]) > 0)
            ok("slack: ts is integer", isinstance(att.get("ts"), int), f"ts={att.get('ts')} type={type(att.get('ts')).__name__}")

            fields = att.get("fields", [])
            titles = [f.get("title", "").lower() for f in fields]
            all_titles = " ".join(titles)
            ok("slack field: Alert ID", "alert id" in all_titles)
            ok("slack field: Failure Rate", "failure rate" in all_titles)
            ok("slack field: Failed Proxies", "failed proxies" in all_titles)
            ok("slack field: Threshold", "threshold" in all_titles)
            ok("slack field: Failed IDs", "failed ids" in all_titles)
            ok("slack field: Fired At", "fired at" in all_titles)

    # DISCORD
    discord_events = [w for w in webhooks_received if w["path"] == "/discord"]
    ok("Discord webhook received", len(discord_events) >= 1, f"got {len(discord_events)}")

    if discord_events:
        d = discord_events[0]["body"]
        ok("discord: has embeds", isinstance(d.get("embeds"), list) and len(d["embeds"]) > 0)

        if d.get("embeds"):
            emb = d["embeds"][0]
            ok("discord: title non-empty", isinstance(emb.get("title"), str) and len(emb["title"]) > 0)
            ok("discord: description non-empty", isinstance(emb.get("description"), str) and len(emb["description"]) > 0)
            ok("discord: color is int", isinstance(emb.get("color"), int), f"color={emb.get('color')} type={type(emb.get('color')).__name__}")
            ok("discord: color in range", 0 <= emb.get("color", -1) <= 16777215)
            ok("discord: footer.text", isinstance(emb.get("footer", {}).get("text"), str) and len(emb["footer"]["text"]) > 0)

            fields = emb.get("fields", [])
            names = [f.get("name", "").lower() for f in fields]
            all_names = " ".join(names)
            ok("discord field: Alert ID", "alert id" in all_names)
            ok("discord field: Failure Rate", "failure rate" in all_names)
            ok("discord field: Failed Proxies", "failed proxies" in all_names)
            ok("discord field: Threshold", "threshold" in all_names)
            ok("discord field: Failed IDs", "failed ids" in all_names)


# ===========================================================================
# CATEGORY 5: Alert resolution (20 pts)
# ===========================================================================
def test_alert_resolution(prev_alert_id: str | None):
    section("Category 5: Alert Resolution (20 pts)")

    clear_webhook_receiver()

    # Replace with all-healthy proxies
    client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/get",
            "https://httpbin.org/ip",
            "https://httpbin.org/headers",
            "https://httpbin.org/user-agent",
        ],
        "replace": True,
    })

    wait_for_check(6)

    r = client.get("/alerts")
    alerts_data = r.json()
    resolved = [a for a in alerts_data if a["status"] == "resolved"]
    ok("resolved alert exists", len(resolved) >= 1, f"resolved count: {len(resolved)}")

    if resolved:
        res = resolved[-1]
        eq("resolved alert status", res["status"], "resolved")
        ok("resolved_at is ISO", ISO_RE.match(res["resolved_at"] or ""))
        ok("resolved_at is not null", res["resolved_at"] is not None)
        if prev_alert_id:
            eq("same alert_id as fired", res["alert_id"], prev_alert_id)

    # No active alerts
    active = [a for a in alerts_data if a["status"] == "active"]
    eq("no active alerts after resolution", len(active), 0)

    # Metrics
    r = client.get("/metrics")
    eq("active_alerts metric is 0", r.json()["active_alerts"], 0)

    # Webhook: alert.resolved delivered
    time.sleep(2)
    webhooks_received = get_webhooks()
    resolved_wh = [w for w in webhooks_received if w["path"] == "/generic" and w["body"].get("event") == "alert.resolved"]
    ok("alert.resolved webhook delivered", len(resolved_wh) >= 1, f"got {len(resolved_wh)}")

    if resolved_wh:
        payload = resolved_wh[0]["body"]
        eq("resolved payload event", payload.get("event"), "alert.resolved")
        ok("resolved payload alert_id", len(payload.get("alert_id", "")) > 0)
        ok("resolved payload resolved_at", ISO_RE.match(payload.get("resolved_at", "")))


# ===========================================================================
# CATEGORY 6: Re-breach lifecycle integrity (30 pts)
# ===========================================================================
def test_rebreach(prev_alert_ids: set):
    section("Category 6: Re-breach Lifecycle Integrity (30 pts)")

    clear_webhook_receiver()

    # Trigger a NEW breach
    client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/get",
            "https://httpbin.org/status/500",
            "https://httpbin.org/status/502",
            "https://httpbin.org/status/503",
        ],
        "replace": True,
    })

    wait_for_check(6)

    r = client.get("/alerts")
    all_alerts = r.json()
    new_active = [a for a in all_alerts if a["status"] == "active"]
    ok("new active alert exists", len(new_active) >= 1)

    if new_active:
        new_alert = new_active[-1]
        ok("new alert_id is brand new", new_alert["alert_id"] not in prev_alert_ids,
           f"new={new_alert['alert_id']} prev={prev_alert_ids}")
        print(f"  New alert_id: {new_alert['alert_id']}")

    # Old alerts preserved
    old_resolved = [a for a in all_alerts if a["status"] == "resolved"]
    ok("previous resolved alerts preserved", len(old_resolved) >= 1)

    # Exactly one active at a time
    eq("only 1 active alert", len(new_active), 1)

    # Webhook: new alert.fired delivered
    time.sleep(2)
    webhooks_received = get_webhooks()
    fired_events = [w for w in webhooks_received if w["path"] == "/generic" and w["body"].get("event") == "alert.fired"]
    ok("new alert.fired webhook delivered", len(fired_events) >= 1)

    return new_active[-1]["alert_id"] if new_active else None


# ===========================================================================
# CATEGORY 7: Pool operations & observability (25 pts)
# ===========================================================================
def test_pool_ops_and_observability():
    section("Category 7: Pool Operations & Observability (25 pts)")

    # Grab alert count before delete
    alerts_before = client.get("/alerts").json()
    alert_count_before = len(alerts_before)

    # Ch8: DELETE /proxies
    r = client.delete("/proxies")
    eq("DELETE /proxies -> 204", r.status_code, 204)
    ok("DELETE body is empty", len(r.content) == 0)

    # Pool is empty
    r = client.get("/proxies")
    data = r.json()
    eq("pool total is 0", data["total"], 0)
    eq("pool up is 0", data["up"], 0)
    eq("pool down is 0", data["down"], 0)
    eq("failure_rate is 0.0", data["failure_rate"], 0.0)
    eq("proxies array empty", data["proxies"], [])

    # Alerts preserved after delete
    r = client.get("/alerts")
    alerts_after = r.json()
    eq("alert count preserved after DELETE", len(alerts_after), alert_count_before)

    # Ch12: GET /metrics
    r = client.get("/metrics")
    eq("GET /metrics -> 200", r.status_code, 200)
    m = r.json()
    ok("total_checks > 0", m["total_checks"] > 0, f"total_checks={m['total_checks']}")
    eq("current_pool_size is 0", m["current_pool_size"], 0)
    ok("total_alerts >= 2", m["total_alerts"] >= 2, f"total_alerts={m['total_alerts']}")
    ok("webhook_deliveries > 0", m["webhook_deliveries"] > 0, f"webhook_deliveries={m['webhook_deliveries']}")

    # Proxy ID edge case: trailing slash
    client.post("/proxies", json={"proxies": ["https://example.com/proxy/px-101/"]})
    r = client.get("/proxies")
    ids = [p["id"] for p in r.json()["proxies"]]
    ok("trailing slash stripped for ID", "px-101" in ids, f"ids={ids}")
    client.delete("/proxies")


# ===========================================================================
# TIMESTAMP FORMAT VALIDATION
# ===========================================================================
def test_timestamps():
    section("Timestamp Format Validation")

    alerts_data = client.get("/alerts").json()
    for a in alerts_data:
        ok(f"fired_at format ({a['alert_id'][:12]})", ISO_RE.match(a["fired_at"]),
           f"got: {a['fired_at']}")
        if a["resolved_at"]:
            ok(f"resolved_at format ({a['alert_id'][:12]})", ISO_RE.match(a["resolved_at"]),
               f"got: {a['resolved_at']}")


# ===========================================================================
# RUN ALL
# ===========================================================================
if __name__ == "__main__":
    print("=" * 65)
    print("  ProxyMaze'26 — Evaluator-Style Black Box Test")
    print("  API:     ", API)
    print("  Webhook: ", WEBHOOK)
    print("=" * 65)

    preflight()

    # Category 1: Bootstrap & Config (10 pts)
    test_bootstrap_and_config()

    # Category 2: Pool ingestion & monitoring (45 pts)
    test_pool_ingestion_and_monitoring()

    # Category 3: Single failure behavior (30 pts)
    test_single_failure()

    # Category 4: Alerts & webhook delivery (90 pts)
    alert = test_alerts_and_webhooks()
    alert_id_1 = alert["alert_id"] if alert else None

    # Bonus: Slack & Discord (+20 pts)
    test_slack_discord_payloads()

    # Category 5: Alert resolution (20 pts)
    test_alert_resolution(alert_id_1)

    # Category 6: Re-breach lifecycle (30 pts)
    prev_ids = {a["alert_id"] for a in client.get("/alerts").json()}
    alert_id_2 = test_rebreach(prev_ids - {alert_id_1} if alert_id_1 else prev_ids)

    # Category 7: Pool ops & observability (25 pts)
    test_pool_ops_and_observability()

    # Timestamps
    test_timestamps()

    # SUMMARY
    total = PASS + FAIL
    print(f"\n{'='*65}")
    print(f"  RESULTS:  {PASS} passed  /  {FAIL} failed  /  {total} total")
    if FAIL == 0:
        print(f"  ALL TESTS PASSED — Ready for submission!")
    else:
        print(f"  {FAIL} tests need fixing before submission.")
    print(f"{'='*65}")

    sys.exit(1 if FAIL else 0)
