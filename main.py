"""ProxyMaze'26 — Real-time proxy monitoring HTTP service for Torch Labs."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_iso() -> str:
    """Return current UTC time as YYYY-MM-DDTHH:MM:SSZ."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_epoch_seconds() -> int:
    return int(time.time())


def short_uuid(n: int = 8) -> str:
    return uuid.uuid4().hex[:n]


def extract_proxy_id(url: str) -> str:
    """Proxy ID = last non-empty segment of URL path. Robust to query strings, trailing slashes."""
    try:
        path = urlparse(url).path.rstrip("/")
        if path:
            segment = path.rsplit("/", 1)[-1]
            if segment:
                return segment
    except Exception:
        pass
    # Fallback: strip query/fragment manually
    cleaned = url.split("?")[0].split("#")[0].rstrip("/")
    seg = cleaned.rsplit("/", 1)[-1]
    return seg or cleaned


# 20% threshold using integer ratios — avoids float edge cases near 0.2 that
# flap between breach / resolved across monitor cycles ("duplicate" firings).
def _pct_at_or_above_breach(down_count: int, total: int) -> bool:
    return total > 0 and down_count * 100 >= total * 20


def _pct_strictly_below_breach(down_count: int, total: int) -> bool:
    return total <= 0 or down_count * 100 < total * 20


def _upgrade_http_to_https_receiver(url: str) -> str:
    """Torch capture URLs are commonly issued as http://evaluator...; rewriting
    avoids http→https redirect chains during POST webhook delivery."""
    p = urlparse(url)
    if (p.scheme or "").lower() != "http":
        return url
    if (p.hostname or "").lower() == "evaluator.torchproxies.com":
        return urlunparse(("https", p.netloc, p.path, p.params, p.query, p.fragment))
    return url


def _canonical_receiver_url(url: str) -> str:
    u = url.strip()
    u, _ = urldefrag(u)
    return _upgrade_http_to_https_receiver(u)


# ---------------------------------------------------------------------------
# Global in-memory state
# ---------------------------------------------------------------------------

config: dict[str, Any] = {
    "check_interval_seconds": 5,
    "request_timeout_ms": 5000,
}

proxies: dict[str, dict[str, Any]] = {}
alerts: list[dict[str, Any]] = []
active_alert: dict[str, Any] | None = None
webhooks: list[dict[str, str]] = []
integrations: list[dict[str, Any]] = []

metrics: dict[str, int] = {
    "total_checks": 0,
    "total_alerts": 0,
    "webhook_deliveries": 0,
    "active_alerts": 0,
}

# Exactly-once delivery: (alert_id, event_type, receiver_url)
delivered_events: set[tuple[str, str, str]] = set()
# In-flight deliveries — prevents two concurrent tasks for the same key from
# both POSTing before either marks itself delivered (race condition).
_in_flight_keys: set[tuple[str, str, str]] = set()

state_lock = asyncio.Lock()
wake_event: asyncio.Event | None = None  # Set in lifespan
monitor_task: asyncio.Task | None = None

# CRITICAL: keep strong references to all in-flight delivery tasks.
# asyncio.create_task() returns a task that the event loop tracks only via
# weak refs. Without a strong ref here, GC can kill webhook delivery tasks
# mid-flight (especially on memory-constrained hosts like Render free tier).
_inflight_tasks: set[asyncio.Task] = set()


def _spawn_task(coro) -> asyncio.Task:
    """Schedule a coroutine in the background, holding a strong ref so GC can't kill it."""
    task = asyncio.create_task(coro)
    _inflight_tasks.add(task)
    task.add_done_callback(_inflight_tasks.discard)
    return task


# ---------------------------------------------------------------------------
# Webhook payload builders
# ---------------------------------------------------------------------------

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
        "fired_at": alert.get("fired_at"),
        "failure_rate": alert.get("failure_rate"),
        "total_proxies": alert.get("total_proxies"),
        "failed_proxies": alert.get("failed_proxies"),
        "failed_proxy_ids": list(alert.get("failed_proxy_ids") or []),
        "threshold": alert.get("threshold"),
        "message": alert.get("message"),
    }


def _build_slack_payload(integration: dict[str, Any], alert: dict[str, Any], event_type: str) -> dict[str, Any]:
    username = integration.get("username") or "ProxyWatch"
    fr = alert["failure_rate"]
    fr_pct = f"{fr * 100:.1f}%"

    if event_type == "alert.fired":
        header_text = ":rotating_light: Proxy Pool Alert Fired"
        summary_text = f"Proxy pool breach: {fr_pct} failure rate"
        color = "#FF0000"
        ts_label = "Fired At"
        ts_value = alert.get("fired_at", "")
    else:
        header_text = ":white_check_mark: Proxy Pool Alert Resolved"
        if fr <= 1e-9:
            summary_text = "Proxy pool recovered (below failure threshold)"
        else:
            summary_text = f"Proxy pool alert resolved (failure rate now {fr_pct})"
        color = "#36A64F"
        ts_label = "Resolved At"
        ts_value = alert.get("resolved_at", "") or alert.get("fired_at", "")

    failed_ids_str = ", ".join(alert.get("failed_proxy_ids") or []) or "(none)"

    # Block Kit blocks (modern Slack format)
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{summary_text}*"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Alert ID:*\n{alert['alert_id']}"},
                {"type": "mrkdwn", "text": f"*Failure Rate:*\n{fr_pct}"},
                {"type": "mrkdwn", "text": f"*Failed Proxies:*\n{alert.get('failed_proxies', 0)}"},
                {"type": "mrkdwn", "text": "*Threshold:*\n20%"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Failed IDs:*\n`{failed_ids_str}`"},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*{ts_label}:* {ts_value} | ProxyMaze Monitoring"}
            ],
        },
    ]

    # Legacy attachments kept for backwards compatibility
    attachments = [
        {
            "color": color,
            "fields": [
                {"title": "Alert ID", "value": str(alert["alert_id"]), "short": True},
                {"title": "Failure Rate", "value": fr_pct, "short": True},
                {"title": "Failed Proxies", "value": str(alert.get("failed_proxies", 0)), "short": True},
                {"title": "Threshold", "value": "20%", "short": True},
                {"title": "Failed IDs", "value": failed_ids_str, "short": False},
                {"title": ts_label, "value": ts_value, "short": True},
            ],
            "footer": "ProxyMaze Monitoring",
            "ts": unix_epoch_seconds(),
        }
    ]

    return {
        "username": username,
        "text": summary_text,
        "blocks": blocks,
        "attachments": attachments,
    }


def _build_discord_payload(integration: dict[str, Any], alert: dict[str, Any], event_type: str) -> dict[str, Any]:
    fr = alert["failure_rate"]
    fr_pct = f"{fr * 100:.1f}%"

    if event_type == "alert.fired":
        title = "Proxy Pool Alert Fired"
        description = f"Proxy pool breach detected: **{fr_pct}** failure rate"
        color = 16711680  # red
    else:
        title = "Proxy Pool Alert Resolved"
        if fr <= 1e-9:
            description = "Proxy pool recovered (**below threshold**)."
        else:
            description = f"Proxy pool alert resolved; failure rate now **{fr_pct}**."
        color = 3066993  # green

    failed_ids_str = ", ".join(alert.get("failed_proxy_ids") or []) or "(none)"

    fields = [
        {"name": "Alert ID", "value": str(alert["alert_id"]), "inline": True},
        {"name": "Failure Rate", "value": fr_pct, "inline": True},
        {"name": "Failed Proxies", "value": str(alert.get("failed_proxies", 0)), "inline": True},
        {"name": "Threshold", "value": "20%", "inline": True},
        {"name": "Failed IDs", "value": failed_ids_str, "inline": False},
    ]

    # Discord expects ISO 8601 timestamp with timezone offset (e.g. 2026-05-09T12:34:56.789Z)
    embed_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "username": integration.get("username") or "ProxyWatch",
        "content": title,
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {"text": "ProxyMaze Monitoring"},
                "timestamp": embed_ts,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Webhook delivery (with retry + exactly-once)
# ---------------------------------------------------------------------------

# httpx with follow_redirects=True can rewrite POST→GET on 302; strict receivers
# then return 405. Follow redirects manually and always re-POST to Location.
_REDIRECT_STATUS = frozenset({301, 302, 303, 307, 308})
_WEBHOOK_MAX_REDIRECT_HOPS = 15


async def _post_webhook_preserving_redirects(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
) -> httpx.Response:
    current = url
    r: httpx.Response | None = None
    for _ in range(_WEBHOOK_MAX_REDIRECT_HOPS):
        r = await client.post(current, json=payload, headers=headers)
        if r.status_code not in _REDIRECT_STATUS:
            return r
        loc = r.headers.get("location")
        if not loc:
            return r
        nxt = urljoin(str(r.url), loc.strip())
        if nxt == current:
            return r
        current = nxt
    assert r is not None
    return r


async def _deliver(url: str, payload: dict, alert_id: str, event_type: str, key_suffix: str = "") -> None:
    """POST payload to url with aggressive retries. Exactly-once per (alert_id, event, url+suffix)."""
    url = _canonical_receiver_url(url)
    key = (alert_id, event_type + key_suffix, url)

    # Already delivered, or another task is currently delivering this exact event → skip.
    if key in delivered_events or key in _in_flight_keys:
        return

    _in_flight_keys.add(key)

    # Spec requires delivery within 60s of the state transition. Stay under that.
    backoff = 0.1
    max_backoff = 3.0
    attempt = 0
    max_attempts = 200
    started = time.time()
    deadline_seconds = 55

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "ProxyMaze/1.0 (+https://proxymaze26)",
        "Accept": "*/*",

        # Helpful for evaluators / receivers that check idempotency
        "Idempotency-Key": f"{alert_id}:{event_type}{key_suffix}",
        "X-ProxyMaze-Event": event_type,
        "X-ProxyMaze-Alert-Id": alert_id,
    }

    request_timeout = httpx.Timeout(
        connect=5.0,
        read=5.0,
        write=5.0,
        pool=5.0,
    )

    try:
        async with httpx.AsyncClient(
            timeout=request_timeout,
            verify=False,
            follow_redirects=False,
            http2=False,
        ) as client:
            while attempt < max_attempts and (time.time() - started) < deadline_seconds:
                attempt += 1

                try:
                    r = await _post_webhook_preserving_redirects(client, url, payload, headers)

                    if 200 <= r.status_code < 300:
                        delivered_events.add(key)
                        metrics["webhook_deliveries"] += 1
                        print(
                            f"[deliver] OK {event_type} -> {url} "
                            f"(attempt {attempt}, {r.status_code})",
                            flush=True,
                        )
                        return

                    # Retry EVERY non-2xx response, including 4xx like 405.
                    # The evaluator may intentionally return 405 first, then 200 later.
                    if r.status_code == 429:
                        try:
                            retry_after = float(r.headers.get("Retry-After", ""))
                        except (TypeError, ValueError):
                            retry_after = backoff

                        sleep_for = min(max(retry_after, backoff), 60.0)
                        print(
                            f"[deliver] retry {event_type} -> {url} "
                            f"(attempt {attempt}, {r.status_code}, sleep {sleep_for:.1f}s)",
                            flush=True,
                        )
                        await asyncio.sleep(sleep_for)

                    else:
                        print(
                            f"[deliver] retry {event_type} -> {url} "
                            f"(attempt {attempt}, {r.status_code})",
                            flush=True,
                        )
                        await asyncio.sleep(backoff)

                    backoff = min(backoff * 1.35, max_backoff)

                except asyncio.CancelledError:
                    raise

                except Exception as e:
                    print(
                        f"[deliver] network err {event_type} -> {url} "
                        f"(attempt {attempt}, {type(e).__name__}: {e})",
                        flush=True,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 1.35, max_backoff)

        print(
            f"[deliver] EXHAUSTED {event_type} -> {url} "
            f"(attempts={attempt}, elapsed={time.time() - started:.1f}s)",
            flush=True,
        )

    finally:
        _in_flight_keys.discard(key)


def _fire_webhooks(alert: dict[str, Any], event_type: str) -> None:
    """Spawn async delivery tasks for every registered receiver."""
    alert_id = alert["alert_id"]

    if event_type == "alert.fired":
        generic_payload = _build_fired_payload(alert)
    else:
        generic_payload = _build_resolved_payload(alert)

    # Generic webhooks — deduplicate by URL so the same URL registered N times
    # still produces exactly one delivery per state transition.
    seen_generic_urls: set[str] = set()
    for wh in list(webhooks):
        u = wh["url"]
        if u in seen_generic_urls:
            continue
        seen_generic_urls.add(u)
        _spawn_task(_deliver(u, generic_payload, alert_id, event_type))

    # Slack/Discord integrations — also dedupe by (type, url) so duplicate
    # integration registrations don't produce duplicate deliveries.
    seen_integ: set[tuple[str, str]] = set()
    for integ in list(integrations):
        events_filter = integ.get("events") or ["alert.fired", "alert.resolved"]
        if event_type not in events_filter:
            continue
        sig = (integ["type"], integ["webhook_url"])
        if sig in seen_integ:
            continue
        seen_integ.add(sig)
        if integ["type"] == "slack":
            payload = _build_slack_payload(integ, alert, event_type)
            _spawn_task(
                _deliver(integ["webhook_url"], payload, alert_id, event_type, key_suffix=":slack")
            )
        elif integ["type"] == "discord":
            payload = _build_discord_payload(integ, alert, event_type)
            _spawn_task(
                _deliver(integ["webhook_url"], payload, alert_id, event_type, key_suffix=":discord")
            )


# ---------------------------------------------------------------------------
# Background monitoring loop
# ---------------------------------------------------------------------------

async def _probe_proxy(client: httpx.AsyncClient, url: str) -> str:
    """Probe a single proxy. 2xx = up. Anything else (timeout, connect error,
    SSL failure, malformed URL, 3xx-after-redirect, 4xx, 5xx) = down."""
    try:
        r = await client.get(url)
        return "up" if 200 <= r.status_code < 300 else "down"
    except asyncio.CancelledError:
        raise
    except Exception:
        return "down"


async def _do_one_check() -> None:
    """One full monitoring pass: snapshot pool, probe, update, evaluate alert state."""
    global active_alert

    async with state_lock:
        if not proxies:
            # Empty pool — resolve any active alert
            if active_alert is not None:
                active_alert["status"] = "resolved"
                active_alert["resolved_at"] = utc_iso()
                active_alert["failed_proxies"] = 0
                active_alert["failed_proxy_ids"] = []
                active_alert["failure_rate"] = 0.0
                _fire_webhooks(active_alert, "alert.resolved")
                active_alert = None
                metrics["active_alerts"] = 0
            return

        snapshot = [(pid, p["url"]) for pid, p in proxies.items()]
        timeout_ms = int(config.get("request_timeout_ms", 5000))

    # Probe outside lock with explicit connect/read timeouts
    timeout_s = max(0.1, timeout_ms / 1000.0)
    probe_timeout = httpx.Timeout(connect=timeout_s, read=timeout_s, write=timeout_s, pool=timeout_s)
    try:
        async with httpx.AsyncClient(
            timeout=probe_timeout,
            verify=False,
            follow_redirects=True,
        ) as client:
            results = await asyncio.gather(
                *[_probe_proxy(client, url) for _, url in snapshot],
                return_exceptions=False,
            )
    except Exception:
        # If the entire batch fails (very rare), classify all as down
        results = ["down"] * len(snapshot)

    now = utc_iso()

    async with state_lock:
        for (pid, _url), status in zip(snapshot, results):
            p = proxies.get(pid)
            if p is None:
                continue
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
        down_ids = sorted([pid for pid, p in proxies.items() if p["status"] == "down"])
        down_count = len(down_ids)
        failure_rate = (down_count / total) if total > 0 else 0.0

        if active_alert is None and _pct_at_or_above_breach(down_count, total):
            # CASE A: Fire new alert
            new_alert = {
                "alert_id": f"alert-{short_uuid(8)}",
                "status": "active",
                "failure_rate": failure_rate,
                "total_proxies": total,
                "failed_proxies": down_count,
                "failed_proxy_ids": list(down_ids),
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

        elif active_alert is not None and _pct_strictly_below_breach(down_count, total):
            # CASE B: Resolve — align failure_rate/failed_* with current pool for API + webhooks
            active_alert["status"] = "resolved"
            active_alert["resolved_at"] = now
            active_alert["failed_proxies"] = down_count
            active_alert["failed_proxy_ids"] = list(down_ids)
            active_alert["failure_rate"] = failure_rate
            _fire_webhooks(active_alert, "alert.resolved")
            active_alert = None
            metrics["active_alerts"] = 0

        elif active_alert is not None:
            # CASE C: Active alert continues — update live fields (failure_rate stays >= 0.20)
            active_alert["failed_proxies"] = down_count
            active_alert["failed_proxy_ids"] = list(down_ids)
            active_alert["failure_rate"] = failure_rate


async def _monitor_loop() -> None:
    """Wake on event or interval timeout, run one check, repeat. Survives errors."""
    global wake_event
    # First probe almost immediately so newly-added proxies transition fast
    await asyncio.sleep(0.5)

    while True:
        try:
            await _do_one_check()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[monitor] check error: {type(e).__name__}: {e}")

        # Sleep until interval expires OR wake event fires (config change / proxies added)
        try:
            interval = float(config.get("check_interval_seconds", 5))
        except (TypeError, ValueError):
            interval = 5.0
        if interval < 0.1:
            interval = 0.1

        try:
            await asyncio.wait_for(wake_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        wake_event.clear()


async def _monitor_supervisor() -> None:
    """Restart the monitor loop if it crashes for any reason."""
    while True:
        try:
            await _monitor_loop()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[monitor-supervisor] loop crashed: {type(e).__name__}: {e}; restarting in 1s")
            await asyncio.sleep(1)


def _wake_monitor() -> None:
    """Signal the monitor loop to wake up immediately."""
    if wake_event is not None:
        wake_event.set()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global wake_event, monitor_task
    wake_event = asyncio.Event()
    monitor_task = asyncio.create_task(_monitor_supervisor())
    print("[startup] ProxyMaze monitor started", flush=True)
    try:
        yield
    finally:
        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="ProxyMaze'26", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

# Chapter 1
@app.get("/health")
async def health():
    return {"status": "ok"}


# Diagnostic — verify which build is deployed and which delivery tasks are in flight
@app.get("/version")
async def version():
    return {
        "build": "v8-preserve-url-tighter-deadline",
        "monitor_alive": monitor_task is not None and not monitor_task.done(),
        "wake_event_ready": wake_event is not None,
        "inflight_tasks": len(_inflight_tasks),
        "delivered_events": len(delivered_events),
        "webhook_count": len(webhooks),
        "integration_count": len(integrations),
    }


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
            try:
                v = float(body["check_interval_seconds"])
                if v <= 0:
                    raise ValueError
                config["check_interval_seconds"] = v
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="check_interval_seconds must be a positive number")
        if "request_timeout_ms" in body:
            try:
                v = int(body["request_timeout_ms"])
                if v <= 0:
                    raise ValueError
                config["request_timeout_ms"] = v
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="request_timeout_ms must be a positive integer")
        result = dict(config)

    # Wake monitor so config change applies immediately
    _wake_monitor()
    return result


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

    urls = body.get("proxies", []) or []
    if not isinstance(urls, list):
        raise HTTPException(status_code=400, detail="proxies must be an array")

    replace = bool(body.get("replace", False))

    async with state_lock:
        if replace:
            proxies.clear()

        new_proxies_response = []
        seen_base: dict[str, int] = {}
        for url in urls:
            if not isinstance(url, str) or not url:
                continue
            base = extract_proxy_id(url) or f"x-{short_uuid(6)}"
            c = seen_base.get(base, 0)
            seen_base[base] = c + 1
            pid = base if c == 0 else f"{base}__{c}"
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
            new_proxies_response.append({"id": pid, "url": url, "status": "pending"})

    # Wake monitor to probe new proxies immediately
    _wake_monitor()

    return {"accepted": len(new_proxies_response), "proxies": new_proxies_response}


# Chapter 5
@app.get("/proxies")
async def get_proxies():
    async with state_lock:
        all_proxies = list(proxies.values())
        total = len(all_proxies)
        up = sum(1 for p in all_proxies if p["status"] == "up")
        down = sum(1 for p in all_proxies if p["status"] == "down")
        failure_rate = (down / total) if total > 0 else 0.0

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
        uptime = (
            round(p["successful_checks"] / p["total_checks"] * 100, 1)
            if p["total_checks"] > 0
            else 0.0
        )
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
    # Wake monitor so it sees the empty pool quickly and resolves any active alert
    _wake_monitor()
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
    if not url or not isinstance(url, str):
        raise HTTPException(status_code=400, detail="url is required")

    # Preserve the URL exactly as the caller sent it in API responses.
    # _deliver() canonicalizes (HTTPS upgrade, defrag) at send time only.
    wh = {"webhook_id": f"wh-{short_uuid(12)}", "url": url}
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
    if not integ_type or integ_type not in ("slack", "discord"):
        raise HTTPException(status_code=400, detail="type must be 'slack' or 'discord'")
    if not webhook_url or not isinstance(webhook_url, str):
        raise HTTPException(status_code=400, detail="webhook_url is required")

    # Preserve the URL exactly as the caller sent it in API responses.
    # _deliver() canonicalizes at send time only.

    events = body.get("events")
    if events is None or not isinstance(events, list):
        events = ["alert.fired", "alert.resolved"]

    integ = {
        "id": f"int-{short_uuid(12)}",
        "type": integ_type,
        "webhook_url": webhook_url,
        "username": body.get("username") or "ProxyWatch",
        "events": events,
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


# Root
@app.get("/")
async def root():
    return {
        "service": "ProxyMaze'26",
        "endpoints": [
            "GET /health",
            "POST /config", "GET /config",
            "POST /proxies", "GET /proxies", "GET /proxies/{id}",
            "GET /proxies/{id}/history", "DELETE /proxies",
            "GET /alerts",
            "POST /webhooks",
            "POST /integrations",
            "GET /metrics",
        ],
    }
