# ProxyMaze'26

Real-time proxy monitoring HTTP service for Torch Labs.

## Run

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Architecture

- **FastAPI** async app, all state in-memory protected by `asyncio.Lock`.
- Background monitoring loop probes all proxies concurrently each cycle.
- Alert state machine: fires at ≥20% failure rate, resolves when <20%.
- Webhook delivery with exponential backoff retry and exactly-once guarantee.
- Slack and Discord integration payloads follow platform-specific formats.

## Test Scenarios

1. POST proxies with httpbin URLs (`/status/200`, `/status/500`), wait for a check cycle, verify GET /proxies reflects real probe results.
2. Trigger a breach (≥20% down), verify GET /alerts shows an active alert and webhooks fire.
3. Resolve the breach, verify alert is resolved and `alert.resolved` webhook fires.
4. DELETE /proxies, verify alerts are preserved.
5. Re-breach produces a new `alert_id`.
