"""
Comprehensive test suite for ProxyMaze'26.
Run the server first: uvicorn main:app --host 0.0.0.0 --port 8000
Then run: python test_all.py
"""

import time
import json
import httpx
import sys

BASE = "http://localhost:8000"
PASS = 0
FAIL = 0


def log(test_name: str, passed: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if passed else "FAIL"
    if passed:
        PASS += 1
    else:
        FAIL += 1
    msg = f"  [{status}] {test_name}"
    if detail and not passed:
        msg += f" -- {detail}"
    print(msg)


def assert_eq(test_name: str, actual, expected):
    log(test_name, actual == expected, f"expected {expected!r}, got {actual!r}")


def assert_true(test_name: str, condition, detail=""):
    log(test_name, bool(condition), detail)


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


client = httpx.Client(base_url=BASE, timeout=30.0)


# =========================================================================
# CHAPTER 1: GET /health
# =========================================================================
def test_health():
    section("Chapter 1: GET /health")
    r = client.get("/health")
    assert_eq("status code 200", r.status_code, 200)
    assert_eq("body has status ok", r.json().get("status"), "ok")


# =========================================================================
# CHAPTER 2 & 3: POST /config + GET /config
# =========================================================================
def test_config():
    section("Chapter 2 & 3: Config")

    # GET default config
    r = client.get("/config")
    assert_eq("GET /config 200", r.status_code, 200)
    data = r.json()
    assert_true("has check_interval_seconds", "check_interval_seconds" in data)
    assert_true("has request_timeout_ms", "request_timeout_ms" in data)

    # POST new config
    r = client.post("/config", json={
        "check_interval_seconds": 3,
        "request_timeout_ms": 3000,
    })
    assert_eq("POST /config 200", r.status_code, 200)
    data = r.json()
    assert_eq("check_interval_seconds updated", data["check_interval_seconds"], 3)
    assert_eq("request_timeout_ms updated", data["request_timeout_ms"], 3000)

    # Verify GET reflects change
    r = client.get("/config")
    assert_eq("GET reflects new interval", r.json()["check_interval_seconds"], 3)

    # Unknown fields accepted silently
    r = client.post("/config", json={
        "check_interval_seconds": 3,
        "unknown_field": "should be ignored",
    })
    assert_eq("unknown fields accepted", r.status_code, 200)

    # Malformed JSON -> 400
    r = client.post("/config", content=b"not json", headers={"Content-Type": "application/json"})
    assert_eq("malformed JSON -> 400", r.status_code, 400)


# =========================================================================
# CHAPTER 4: POST /proxies
# =========================================================================
def test_post_proxies():
    section("Chapter 4: POST /proxies")

    # First, clean slate
    client.delete("/proxies")

    # Add proxies
    urls = [
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/500",
        "https://httpbin.org/delay/10",  # will timeout with 3s timeout
    ]
    r = client.post("/proxies", json={"proxies": urls})
    assert_eq("POST /proxies 201", r.status_code, 201)
    data = r.json()
    assert_eq("accepted count", data["accepted"], 3)
    assert_eq("proxy count in response", len(data["proxies"]), 3)

    # Check IDs are deterministic (last path segment)
    ids = [p["id"] for p in data["proxies"]]
    assert_eq("ID from /status/200", ids[0], "200")
    assert_eq("ID from /status/500", ids[1], "500")
    assert_eq("ID from /delay/10", ids[2], "10")

    # All start as pending
    for p in data["proxies"]:
        assert_eq(f"proxy {p['id']} starts pending", p["status"], "pending")

    # Unknown fields accepted
    r = client.post("/proxies", json={"proxies": [], "extra_field": True})
    assert_eq("unknown fields accepted", r.status_code, 201)

    # Replace mode
    r = client.post("/proxies", json={
        "proxies": ["https://httpbin.org/status/200"],
        "replace": True,
    })
    assert_eq("replace mode 201", r.status_code, 201)
    assert_eq("replace accepted 1", r.json()["accepted"], 1)

    # Verify only 1 proxy now
    r = client.get("/proxies")
    assert_eq("only 1 proxy after replace", r.json()["total"], 1)


# =========================================================================
# CHAPTER 5: GET /proxies (before any checks)
# =========================================================================
def test_get_proxies_initial():
    section("Chapter 5: GET /proxies (initial state)")

    # Reset with known proxies
    client.delete("/proxies")
    client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/status/500",
        ]
    })

    r = client.get("/proxies")
    assert_eq("status 200", r.status_code, 200)
    data = r.json()
    assert_eq("total is 2", data["total"], 2)
    assert_true("has failure_rate", "failure_rate" in data)
    assert_true("has proxies array", isinstance(data["proxies"], list))

    for p in data["proxies"]:
        assert_true(f"proxy {p['id']} has required fields",
                    all(k in p for k in ["id", "url", "status", "last_checked_at", "consecutive_failures"]))


# =========================================================================
# CHAPTER 6 & 7: GET /proxies/{id} and GET /proxies/{id}/history
# =========================================================================
def test_get_proxy_detail():
    section("Chapter 6 & 7: Proxy detail and history")

    # Known proxy
    r = client.get("/proxies/200")
    assert_eq("known proxy 200", r.status_code, 200)
    data = r.json()
    assert_eq("id matches", data["id"], "200")
    assert_true("has uptime_percentage", "uptime_percentage" in data)
    assert_true("has total_checks", "total_checks" in data)
    assert_true("has history array", isinstance(data["history"], list))

    # Unknown proxy -> 404
    r = client.get("/proxies/nonexistent")
    assert_eq("unknown proxy 404", r.status_code, 404)

    # History endpoint
    r = client.get("/proxies/200/history")
    assert_eq("history 200", r.status_code, 200)
    assert_true("history is array", isinstance(r.json(), list))

    # Unknown history -> 404
    r = client.get("/proxies/nonexistent/history")
    assert_eq("unknown history 404", r.status_code, 404)


# =========================================================================
# CHAPTER 8: DELETE /proxies
# =========================================================================
def test_delete_proxies():
    section("Chapter 8: DELETE /proxies")

    r = client.delete("/proxies")
    assert_eq("DELETE 204", r.status_code, 204)
    assert_true("no body", len(r.content) == 0)

    # Pool is empty
    r = client.get("/proxies")
    assert_eq("pool empty after delete", r.json()["total"], 0)


# =========================================================================
# CHAPTER 9: GET /alerts (initially empty)
# =========================================================================
def test_alerts_initial():
    section("Chapter 9: GET /alerts (initial)")

    r = client.get("/alerts")
    assert_eq("alerts 200", r.status_code, 200)
    assert_true("alerts is array", isinstance(r.json(), list))


# =========================================================================
# CHAPTER 10: POST /webhooks
# =========================================================================
def test_webhooks():
    section("Chapter 10: POST /webhooks")

    r = client.post("/webhooks", json={"url": "https://webhook.site/test-receiver"})
    assert_eq("webhook 201", r.status_code, 201)
    data = r.json()
    assert_true("has webhook_id", "webhook_id" in data)
    assert_true("webhook_id starts with wh-", data["webhook_id"].startswith("wh-"))
    assert_eq("url matches", data["url"], "https://webhook.site/test-receiver")

    # Unknown fields accepted
    r = client.post("/webhooks", json={"url": "https://example.com", "extra": True})
    assert_eq("unknown fields accepted", r.status_code, 201)


# =========================================================================
# CHAPTER 11: POST /integrations
# =========================================================================
def test_integrations():
    section("Chapter 11: POST /integrations")

    # Slack
    r = client.post("/integrations", json={
        "type": "slack",
        "webhook_url": "https://hooks.slack.com/test",
        "username": "TestBot",
        "events": ["alert.fired", "alert.resolved"],
    })
    assert_eq("slack integration 201", r.status_code, 201)
    data = r.json()
    assert_true("has id", "id" in data)
    assert_eq("type is slack", data["type"], "slack")
    assert_eq("username stored", data["username"], "TestBot")

    # Discord
    r = client.post("/integrations", json={
        "type": "discord",
        "webhook_url": "https://discord.com/api/webhooks/test",
        "events": ["alert.fired", "alert.resolved"],
    })
    assert_eq("discord integration 201", r.status_code, 201)
    assert_eq("type is discord", r.json()["type"], "discord")

    # Unknown fields
    r = client.post("/integrations", json={
        "type": "slack",
        "webhook_url": "https://example.com",
        "extra_field": 123,
    })
    assert_eq("unknown fields accepted", r.status_code, 201)


# =========================================================================
# CHAPTER 12: GET /metrics
# =========================================================================
def test_metrics():
    section("Chapter 12: GET /metrics")

    r = client.get("/metrics")
    assert_eq("metrics 200", r.status_code, 200)
    data = r.json()
    for key in ["total_checks", "current_pool_size", "active_alerts", "total_alerts", "webhook_deliveries"]:
        assert_true(f"has {key}", key in data, f"missing key: {key}")


# =========================================================================
# BACKGROUND MONITORING + ALERT STATE MACHINE (integration test)
# =========================================================================
def test_monitoring_and_alerts():
    section("Background Monitoring + Alert State Machine")

    # Set fast config for testing
    client.post("/config", json={"check_interval_seconds": 3, "request_timeout_ms": 3000})

    # Clean slate
    client.delete("/proxies")

    # Add proxies: 3 will be up (200), 2 will be down (500 + timeout)
    # That's 2/5 = 40% failure -> should trigger alert
    proxies = [
        "https://httpbin.org/status/200",        # id: 200 -> up
        "https://httpbin.org/get",                # id: get -> up
        "https://httpbin.org/ip",                 # id: ip  -> up
        "https://httpbin.org/status/500",         # id: 500 -> down
        "https://httpbin.org/status/503",         # id: 503 -> down
    ]
    r = client.post("/proxies", json={"proxies": proxies, "replace": True})
    assert_eq("added 5 proxies", r.json()["accepted"], 5)

    # All should be pending before first check
    r = client.get("/proxies")
    pending_count = sum(1 for p in r.json()["proxies"] if p["status"] == "pending")
    assert_eq("all pending before check", pending_count, 5)

    print("\n  Waiting for first background check cycle (~4 seconds)...")
    time.sleep(5)

    # After first check, statuses should be real
    r = client.get("/proxies")
    data = r.json()
    print(f"  Pool state: total={data['total']}, up={data['up']}, down={data['down']}, "
          f"failure_rate={data['failure_rate']:.2f}")

    assert_eq("total still 5", data["total"], 5)
    assert_true("some are up", data["up"] > 0, f"up={data['up']}")
    assert_true("some are down", data["down"] > 0, f"down={data['down']}")
    assert_true("no more pending", all(p["status"] != "pending" for p in data["proxies"]),
                f"statuses: {[p['status'] for p in data['proxies']]}")

    # Check individual proxy detail
    r = client.get("/proxies/200")
    if r.status_code == 200:
        detail = r.json()
        assert_eq("proxy 200 is up", detail["status"], "up")
        assert_true("total_checks >= 1", detail["total_checks"] >= 1)
        assert_true("has last_checked_at", detail["last_checked_at"] is not None)
        assert_true("uptime_percentage > 0", detail["uptime_percentage"] > 0)
        assert_true("history has entries", len(detail["history"]) >= 1)

    r = client.get("/proxies/500")
    if r.status_code == 200:
        detail = r.json()
        assert_eq("proxy 500 is down", detail["status"], "down")
        assert_true("consecutive_failures >= 1", detail["consecutive_failures"] >= 1)

    # Check history endpoint
    r = client.get("/proxies/200/history")
    assert_eq("history 200", r.status_code, 200)
    history = r.json()
    assert_true("history is list", isinstance(history, list))
    assert_true("history has entries", len(history) >= 1)
    for entry in history:
        assert_true("entry has checked_at", "checked_at" in entry)
        assert_true("entry has status", "status" in entry)
        assert_true("timestamp format", entry["checked_at"].endswith("Z"))

    # Check alert was fired (failure_rate = 2/5 = 0.40 >= 0.20)
    r = client.get("/alerts")
    alerts = r.json()
    assert_true("alert was fired", len(alerts) >= 1, f"alerts count: {len(alerts)}")

    if alerts:
        alert = alerts[-1]
        print(f"  Alert: id={alert['alert_id']}, status={alert['status']}, "
              f"failure_rate={alert['failure_rate']:.2f}")
        assert_eq("alert status active", alert["status"], "active")
        assert_true("failure_rate >= 0.20", alert["failure_rate"] >= 0.20)
        assert_eq("threshold is 0.2", alert["threshold"], 0.2)
        assert_true("has fired_at", alert["fired_at"] is not None)
        assert_eq("resolved_at is null", alert["resolved_at"], None)
        assert_true("has alert_id", len(alert["alert_id"]) > 0)
        assert_true("has message", len(alert["message"]) > 0)
        assert_true("failed_proxy_ids is list", isinstance(alert["failed_proxy_ids"], list))
        assert_true("failed_proxy_ids not empty", len(alert["failed_proxy_ids"]) > 0)
        assert_eq("total_proxies matches", alert["total_proxies"], 5)

        # Consistency: failed_proxy_ids should match GET /proxies down list
        proxies_data = client.get("/proxies").json()
        down_ids_from_proxies = sorted([p["id"] for p in proxies_data["proxies"] if p["status"] == "down"])
        down_ids_from_alert = sorted(alert["failed_proxy_ids"])
        assert_eq("consistency: failed IDs match", down_ids_from_alert, down_ids_from_proxies)

    # Check metrics updated
    r = client.get("/metrics")
    m = r.json()
    assert_true("total_checks > 0", m["total_checks"] > 0)
    assert_eq("current_pool_size is 5", m["current_pool_size"], 5)
    assert_eq("active_alerts is 1", m["active_alerts"], 1)
    assert_true("total_alerts >= 1", m["total_alerts"] >= 1)


# =========================================================================
# ALERT RESOLUTION TEST
# =========================================================================
def test_alert_resolution():
    section("Alert Resolution")

    # Replace pool with all-healthy proxies -> should resolve alert
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

    print("  Waiting for check cycle to resolve alert (~4 seconds)...")
    time.sleep(5)

    r = client.get("/alerts")
    alerts = r.json()
    resolved = [a for a in alerts if a["status"] == "resolved"]
    assert_true("at least one resolved alert", len(resolved) >= 1,
                f"resolved count: {len(resolved)}")

    if resolved:
        a = resolved[-1]
        assert_true("resolved_at is set", a["resolved_at"] is not None)
        print(f"  Resolved alert: {a['alert_id']}, resolved_at={a['resolved_at']}")

    r = client.get("/metrics")
    assert_eq("active_alerts back to 0", r.json()["active_alerts"], 0)


# =========================================================================
# RE-BREACH: new alert_id
# =========================================================================
def test_rebreach():
    section("Re-breach (new alert_id)")

    # Get current alert IDs
    old_alerts = client.get("/alerts").json()
    old_ids = {a["alert_id"] for a in old_alerts}

    # Introduce failures again
    client.post("/proxies", json={
        "proxies": [
            "https://httpbin.org/status/200",
            "https://httpbin.org/status/500",
            "https://httpbin.org/status/502",
            "https://httpbin.org/status/503",
        ],
        "replace": True,
    })

    print("  Waiting for check cycle to fire new alert (~4 seconds)...")
    time.sleep(5)

    r = client.get("/alerts")
    all_alerts = r.json()
    new_alerts = [a for a in all_alerts if a["alert_id"] not in old_ids]
    assert_true("new alert fired", len(new_alerts) >= 1,
                f"new alert count: {len(new_alerts)}")

    if new_alerts:
        new_alert = new_alerts[-1]
        assert_eq("new alert is active", new_alert["status"], "active")
        assert_true("new alert_id is unique", new_alert["alert_id"] not in old_ids)
        print(f"  New alert_id: {new_alert['alert_id']}")

    # Old alerts should be preserved
    assert_true("old alerts preserved", len(all_alerts) >= len(old_alerts),
                f"total alerts: {len(all_alerts)} vs old: {len(old_alerts)}")


# =========================================================================
# DELETE /proxies PRESERVES ALERTS
# =========================================================================
def test_delete_preserves_alerts():
    section("DELETE /proxies preserves alerts")

    alerts_before = client.get("/alerts").json()
    alert_count_before = len(alerts_before)

    client.delete("/proxies")

    alerts_after = client.get("/alerts").json()
    assert_eq("alert count preserved", len(alerts_after), alert_count_before)

    # Pool should be empty
    r = client.get("/proxies")
    assert_eq("pool empty", r.json()["total"], 0)
    assert_eq("failure_rate 0.0", r.json()["failure_rate"], 0.0)


# =========================================================================
# TIMESTAMP FORMAT VALIDATION
# =========================================================================
def test_timestamp_format():
    section("Timestamp format validation")

    import re
    iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    alerts = client.get("/alerts").json()
    for a in alerts:
        assert_true(f"fired_at format ({a['alert_id']})",
                    iso_pattern.match(a["fired_at"]),
                    f"got: {a['fired_at']}")
        if a["resolved_at"]:
            assert_true(f"resolved_at format ({a['alert_id']})",
                        iso_pattern.match(a["resolved_at"]),
                        f"got: {a['resolved_at']}")


# =========================================================================
# PROXY ID EXTRACTION EDGE CASES
# =========================================================================
def test_proxy_id_extraction():
    section("Proxy ID extraction")

    client.delete("/proxies")

    # Trailing slash
    r = client.post("/proxies", json={
        "proxies": ["https://example.com/proxy/px-101/"]
    })
    ids = [p["id"] for p in r.json()["proxies"]]
    assert_eq("trailing slash stripped", ids[0], "px-101")

    # Simple path
    client.delete("/proxies")
    r = client.post("/proxies", json={
        "proxies": ["https://proxy-provider.example/proxy/px-202"]
    })
    assert_eq("simple path ID", r.json()["proxies"][0]["id"], "px-202")


# =========================================================================
# EDGE CASE: empty pool metrics
# =========================================================================
def test_empty_pool():
    section("Edge case: empty pool")

    client.delete("/proxies")

    r = client.get("/proxies")
    data = r.json()
    assert_eq("total 0", data["total"], 0)
    assert_eq("up 0", data["up"], 0)
    assert_eq("down 0", data["down"], 0)
    assert_eq("failure_rate 0.0", data["failure_rate"], 0.0)
    assert_eq("proxies empty", data["proxies"], [])

    r = client.get("/metrics")
    assert_eq("pool_size 0", r.json()["current_pool_size"], 0)


# =========================================================================
# RUN ALL TESTS
# =========================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  ProxyMaze'26 — Full Test Suite")
    print("  Server must be running at", BASE)
    print("=" * 60)

    # Check server is up
    try:
        r = client.get("/health")
        if r.status_code != 200:
            print("ERROR: Server not healthy")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Cannot connect to server at {BASE}")
        print(f"  {e}")
        print("  Start the server first: uvicorn main:app --host 0.0.0.0 --port 8000")
        sys.exit(1)

    # Fast tests (no waiting)
    test_health()
    test_config()
    test_post_proxies()
    test_get_proxies_initial()
    test_get_proxy_detail()
    test_delete_proxies()
    test_alerts_initial()
    test_webhooks()
    test_integrations()
    test_metrics()
    test_proxy_id_extraction()
    test_empty_pool()

    # Slow tests (require background check cycles)
    test_monitoring_and_alerts()
    test_alert_resolution()
    test_rebreach()
    test_delete_preserves_alerts()
    test_timestamp_format()

    # Summary
    print(f"\n{'='*60}")
    print(f"  RESULTS: {PASS} passed, {FAIL} failed, {PASS+FAIL} total")
    print(f"{'='*60}")

    if FAIL > 0:
        sys.exit(1)
