"""
Local webhook receiver — captures all incoming POST requests.
Run this BEFORE the evaluator test.
    python webhook_receiver.py
Runs on port 9000.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import json

app = FastAPI()

# Store all received payloads
received: list[dict] = []


@app.post("/{path:path}")
async def catch_all(path: str, request: Request):
    body = await request.json()
    entry = {"path": f"/{path}", "body": body}
    received.append(entry)
    print(f"  [WEBHOOK RECEIVED] /{path} -> event={body.get('event', body.get('embeds', [{}])[0].get('title', body.get('text', '?')))}")
    return {"ok": True}


@app.get("/received")
async def get_received():
    """Retrieve all captured payloads."""
    return received


@app.delete("/received")
async def clear_received():
    """Clear captured payloads."""
    received.clear()
    return {"cleared": True}


if __name__ == "__main__":
    print("Webhook receiver starting on http://localhost:9000")
    print("Endpoints:")
    print("  POST /*          — captures any webhook payload")
    print("  GET  /received   — view all captured payloads")
    print("  DELETE /received — clear captured payloads")
    print()
    uvicorn.run(app, host="0.0.0.0", port=9000)
