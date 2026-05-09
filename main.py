"""ProxyMaze'26 — Real-time proxy monitoring HTTP service for Torch Labs."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_iso() -> str:
    """Return current UTC time as YYYY-MM-DDTHH:MM:SSZ."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_epoch_seconds() -> int:
    """Integer Unix epoch seconds for Slack ts field."""
    return int(time.time())


def extract_proxy_id(url: str) -> str:
    """Deterministic proxy ID = last path segment of URL (strip trailing slashes)."""
    stripped = url.rstrip("/")
    return stripped.rsplit("/", 1)[-1]


def short_uuid() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Global in-memory state
# ---------------------------------------------------------------------------

config: dict[str, int] = {
    "check_interval_seconds": 30,
    "request_timeout_ms": 5000,
}

# proxy_id -> proxy dict
proxies: dict[str, dict[str, Any]] = {}

alerts: list[dict[str, Any]] = []
active_alert: dict[str, Any] | None = None

webhooks: list[dict[str, str]] = []           # generic webhook receivers
integrations: list[dict[str, Any]] = []       # Slack / Discord

metrics: dict[str, int] = {
    "total_checks": 0,
    "total_alerts": 0,
    "webhook_deliveries": 0,
    "active_alerts": 0,
}

# Exactly-once delivery: (alert_id, event_type, receiver_url)
delivered_events: set[tuple[str, str, str]] = set()

state_lock = asyncio.Lock()
monitor_task: asyncio.Task | None = None


# ---------------------------------------------------------------------------
# Webhook delivery (with retry + exactly-once)
# ---------------------------------------------------------------------------

async def _deliver(url: str, payload: dict, alert_id: str, event_type: str) -> None:
    """POST payload to url with retries. Exactly-once per (alert_id, event, url)."""
    key = (alert_id, event_type, url)
    # Gate: already delivered?
    if key in delivered_events:
        return

    backoff = 1.0
    max_backoff = 10.0
    while True:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
            if 200 <= r.status_code < 300:
                # Success
                delivered_events.add(key)
                metrics["webhook_deliveries"] += 1
                return
            if r.status_code in (500, 502, 503, 504, 408, 429):
                # Retryable
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            # Non-retryable client error (4xx except 408/429) — give up
            return
        except Exception:
            # Connection error / timeout — retry
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def _build_fired_payload(alert: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": "alert.fired",
        "alert_id": alert["alert_id"],
        "fired_at": alert["fired_at"],
        "failure_rate": alert["failure_rate"],
        "total_proxies": alert["total_proxies"],
        "failed_proxies": alert["failed_proxies"],
        "failed_proxy_ids": list(alert["failed_proxy_ids"]),
        "threshold": alert["threshold"],
        "message": alert["message"],
    }


def _build_resolved_payload(alert: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": "alert.resolved",
        "alert_id": alert["alert_id"],
        "resolved_at": alert["resolved_at"],
    }


def _build_slack_payload(
    integration: dict[str, Any],
    alert: dict[str, Any],
    event_type: str,
) -> dict[str, Any]:
    username = integration.get("username") or "ProxyWatch"
    fr = alert["failure_rate"]
    fr_pct = f"{fr * 100:.1f}%"

    if event_type == "alert.fired":
        text = f"Proxy pool breach: {fr_pct} failure rate"
        color = "#FF0000"
    else:
        text = f"Proxy pool alert resolved (was {fr_pct} failure rate)"
        color = "#36A64F"

    fields = [
        {"title": "Alert ID", "value": str(alert["alert_id"])},
        {"title": "Failure Rate", "value": fr_pct},
        {"title": "Failed Proxies", "value": str(alert["failed_proxies"])},
        {"title": "Threshold", "value": "20%"},
        {"title": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"])},
        {"title": "Fired At", "value": alert["fired_at"]},
    ]

    return {
        "username": username,
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze Monitoring",
                "ts": unix_epoch_seconds(),
            }
        ],
    }


def _build_discord_payload(
    alert: dict[str, Any],
    event_type: str,
) -> dict[str, Any]:
    fr = alert["failure_rate"]
    fr_pct = f"{fr * 100:.1f}%"

    if event_type == "alert.fired":
        title = "Proxy Pool Alert Fired"
        description = f"Proxy pool breach detected: {fr_pct} failure rate"
        color = 16711680  # red
    else:
        title = "Proxy Pool Alert Resolved"
        description = f"Proxy pool alert resolved (was {fr_pct} failure rate)"
        color = 3066993   # green

    fields = [
        {"name": "Alert ID", "value": str(alert["alert_id"])},
        {"name": "Failure Rate", "value": fr_pct},
        {"name": "Failed Proxies", "value": str(alert["failed_proxies"])},
        {"name": "Threshold", "value": "20%"},
        {"name": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"])},
    ]

    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {"text": "ProxyMaze Monitoring"},
            }
        ],
    }


def _fire_webhooks(alert: dict[str, Any], event_type: str) -> None:
    """Spawn async delivery tasks for all receivers. Called under lock."""
    alert_id = alert["alert_id"]

    if event_type == "alert.fired":
        generic_payload = _build_fired_payload(alert)
    else:
        generic_payload = _build_resolved_payload(alert)

    # Generic webhooks
    for wh in webhooks:
        asyncio.create_task(_deliver(wh["url"], generic_payload, alert_id, event_type))

    # Slack integrations
    for integ in integrations:
        if event_type not in integ.get("events", []):
            continue
        if integ["type"] == "slack":
            payload = _build_slack_payload(integ, alert, event_type)
            asyncio.create_task(_deliver(integ["webhook_url"], payload, alert_id, f"{event_type}:slack:{integ['id']}"))
        elif integ["type"] == "discord":
            payload = _build_discord_payload(alert, event_type)
            asyncio.create_task(_deliver(integ["webhook_url"], payload, alert_id, f"{event_type}:discord:{integ['id']}"))


# ---------------------------------------------------------------------------
# Background monitoring loop
# ---------------------------------------------------------------------------

async def _probe_proxy(url: str, timeout_ms: int) -> str:
    """Probe a single proxy URL. Return 'up' or 'down'."""
    try:
        async with httpx.AsyncClient(timeout=timeout_ms / 1000.0) as client:
            r = await client.get(url)
        if 200 <= r.status_code < 300:
            return "up"
        return "down"
    except Exception:
        return "down"


async def _monitor_loop() -> None:
    global active_alert

    while True:
        # Sleep in small increments so config changes apply quickly
        elapsed = 0.0
        while elapsed < config["check_interval_seconds"]:
            await asyncio.sleep(0.5)
            elapsed += 0.5

        async with state_lock:
            if not proxies:
                # Empty pool — if there's an active alert, resolve it
                if active_alert is not None:
                    active_alert["status"] = "resolved"
                    active_alert["resolved_at"] = utc_iso()
                    active_alert["failure_rate"] = 0.0
                    active_alert["failed_proxies"] = 0
                    active_alert["failed_proxy_ids"] = []
                    _fire_webhooks(active_alert, "alert.resolved")
                    active_alert = None
                    metrics["active_alerts"] = 0
                continue

            # Snapshot proxy IDs and URLs
            snapshot = [(pid, p["url"]) for pid, p in proxies.items()]
            timeout_ms = config["request_timeout_ms"]

        # Probe concurrently (outside lock for network I/O)
        probe_tasks = [_probe_proxy(url, timeout_ms) for _, url in snapshot]
        results = await asyncio.gather(*probe_tasks)

        now = utc_iso()

        async with state_lock:
            for (pid, _url), status in zip(snapshot, results):
                p = proxies.get(pid)
                if p is None:
                    continue  # proxy was removed during probe
                p["status"] = status
                p["last_checked_at"] = now
                p["total_checks"] += 1
                if status == "up":
                    p["successful_checks"] += 1
                    p["consecutive_failures"] = 0
                else:
                    p["consecutive_failures"] += 1
                p["history"].append({"checked_at": now, "status": status})

            metrics["total_checks"] += len(snapshot)

            # --- Alert state machine ---
            total = len(proxies)
            down_ids = [pid for pid, p in proxies.items() if p["status"] == "down"]
            down_count = len(down_ids)
            failure_rate = down_count / total if total > 0 else 0.0

            if active_alert is None and failure_rate >= 0.20:
                # CASE A: Fire new alert
                new_alert: dict[str, Any] = {
                    "alert_id": f"alert-{uuid.uuid4().hex[:6]}",
                    "status": "active",
                    "failure_rate": failure_rate,
                    "total_proxies": total,
                    "failed_proxies": down_count,
                    "failed_proxy_ids": down_ids,
                    "threshold": 0.2,
                    "fired_at": now,
                    "resolved_at": None,
                    "message": "Proxy pool failure rate exceeded threshold",
                }
                alerts.append(new_alert)
                active_alert = new_alert
                metrics["total_alerts"] += 1
                metrics["active_alerts"] = 1
                _fire_webhooks(active_alert, "alert.fired")

            elif active_alert is not None and failure_rate < 0.20:
                # CASE B: Resolve
                active_alert["status"] = "resolved"
                active_alert["resolved_at"] = now
                active_alert["failed_proxies"] = down_count
                active_alert["failed_proxy_ids"] = down_ids
                active_alert["failure_rate"] = failure_rate
                _fire_webhooks(active_alert, "alert.resolved")
                active_alert = None
                metrics["active_alerts"] = 0

            elif active_alert is not None:
                # CASE C: Update active alert's live fields
                active_alert["failed_proxies"] = down_count
                active_alert["failed_proxy_ids"] = down_ids
                active_alert["failure_rate"] = failure_rate


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task
    monitor_task = asyncio.create_task(_monitor_loop())
    yield
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="ProxyMaze'26", lifespan=lifespan)




# Chapter 1
@app.get("/health")
async def health():
    return {"status": "ok"}


# Chapter 2
@app.post("/config")
async def post_config(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    async with state_lock:
        if "check_interval_seconds" in body:
            config["check_interval_seconds"] = int(body["check_interval_seconds"])
        if "request_timeout_ms" in body:
            config["request_timeout_ms"] = int(body["request_timeout_ms"])
        return dict(config)


# Chapter 3
@app.get("/config")
async def get_config():
    return dict(config)


# Chapter 4
@app.post("/proxies", status_code=201)
async def post_proxies(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    urls = body.get("proxies", [])
    replace = body.get("replace", False)

    async with state_lock:
        if replace:
            proxies.clear()

        new_proxies = []
        for url in urls:
            pid = extract_proxy_id(url)
            p = {
                "id": pid,
                "url": url,
                "status": "pending",
                "last_checked_at": None,
                "consecutive_failures": 0,
                "total_checks": 0,
                "successful_checks": 0,
                "history": [],
            }
            proxies[pid] = p
            new_proxies.append({"id": pid, "url": url, "status": "pending"})

    return {"accepted": len(new_proxies), "proxies": new_proxies}


# Chapter 5
@app.get("/proxies")
async def get_proxies():
    async with state_lock:
        all_proxies = list(proxies.values())
        total = len(all_proxies)
        up = sum(1 for p in all_proxies if p["status"] == "up")
        down = sum(1 for p in all_proxies if p["status"] == "down")
        failure_rate = down / total if total > 0 else 0.0

        proxy_list = [
            {
                "id": p["id"],
                "url": p["url"],
                "status": p["status"],
                "last_checked_at": p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            }
            for p in all_proxies
        ]

    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": failure_rate,
        "proxies": proxy_list,
    }


# Chapter 6
@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    async with state_lock:
        p = proxies.get(proxy_id)
        if p is None:
            raise HTTPException(status_code=404, detail="Proxy not found")
        uptime = round(p["successful_checks"] / p["total_checks"] * 100, 1) if p["total_checks"] > 0 else 0.0
        return {
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
            "total_checks": p["total_checks"],
            "uptime_percentage": uptime,
            "history": list(p["history"]),
        }


# Chapter 7
@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    async with state_lock:
        p = proxies.get(proxy_id)
        if p is None:
            raise HTTPException(status_code=404, detail="Proxy not found")
        return list(p["history"])


# Chapter 8
@app.delete("/proxies", status_code=204)
async def delete_proxies():
    async with state_lock:
        proxies.clear()
    return Response(status_code=204)


# Chapter 9
@app.get("/alerts")
async def get_alerts():
    async with state_lock:
        return [
            {
                "alert_id": a["alert_id"],
                "status": a["status"],
                "failure_rate": a["failure_rate"],
                "total_proxies": a["total_proxies"],
                "failed_proxies": a["failed_proxies"],
                "failed_proxy_ids": list(a["failed_proxy_ids"]),
                "threshold": a["threshold"],
                "fired_at": a["fired_at"],
                "resolved_at": a["resolved_at"],
                "message": a["message"],
            }
            for a in alerts
        ]


# Chapter 10
@app.post("/webhooks", status_code=201)
async def post_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")

    wh = {"webhook_id": f"wh-{short_uuid()}", "url": url}
    async with state_lock:
        webhooks.append(wh)
    return dict(wh)


# Chapter 11
@app.post("/integrations", status_code=201)
async def post_integration(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    integ_type = body.get("type")
    webhook_url = body.get("webhook_url")
    if not integ_type or not webhook_url:
        raise HTTPException(status_code=400, detail="type and webhook_url are required")

    integ = {
        "id": f"int-{short_uuid()}",
        "type": integ_type,
        "webhook_url": webhook_url,
        "username": body.get("username", "ProxyWatch"),
        "events": body.get("events", ["alert.fired", "alert.resolved"]),
    }
    async with state_lock:
        integrations.append(integ)
    return dict(integ)


# Chapter 12
@app.get("/metrics")
async def get_metrics():
    async with state_lock:
        return {
            "total_checks": metrics["total_checks"],
            "current_pool_size": len(proxies),
            "active_alerts": metrics["active_alerts"],
            "total_alerts": metrics["total_alerts"],
            "webhook_deliveries": metrics["webhook_deliveries"],
        }
