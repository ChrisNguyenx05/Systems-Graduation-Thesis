"""Standard-library HTTP entry point for SYS-06 v2."""

import asyncio
import json
import os
import secrets
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from pydantic import ValidationError

from src.alert_dispatcher.channels.dry_run import DryRunChannel
from src.alert_dispatcher.channels.slack import SlackChannel
from src.alert_dispatcher.channels.telegram import TelegramChannel
from src.alert_dispatcher.models import AlertPayload
from src.alert_dispatcher.v2_service import AlertDispatcherV2

load_dotenv()
DRY_RUN = os.getenv("ALERT_DRY_RUN", "true").lower() in {"true", "1", "yes"}


def build_service() -> AlertDispatcherV2:
    if DRY_RUN:
        dry_run = DryRunChannel()
        channels = {"console": dry_run, "telegram": dry_run, "slack": dry_run}
    else:
        channels = {"console": DryRunChannel(), "telegram": TelegramChannel(), "slack": SlackChannel()}
    return AlertDispatcherV2(channels)


SERVICE = build_service()


class AlertHandler(BaseHTTPRequestHandler):
    server_version = "SYS06AlertV2/2.0"

    def _json(self, status: HTTPStatus, data: dict) -> None:
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"dry_run": DRY_RUN, **SERVICE.health()})
            return
        self._json(HTTPStatus.NOT_FOUND, {"detail": "Not found"})

    def do_POST(self) -> None:
        if self.path != "/alerts":
            self._json(HTTPStatus.NOT_FOUND, {"detail": "Not found"})
            return
        expected = os.getenv("ALERT_API_TOKEN", "").strip()
        supplied = self.headers.get("X-Alert-Token", "")
        if not DRY_RUN and (not expected or not secrets.compare_digest(supplied, expected)):
            self._json(HTTPStatus.UNAUTHORIZED, {"detail": "Invalid alert API token"})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 65_536:
                raise ValueError("Request body must be between 1 and 65536 bytes")
            payload = AlertPayload.model_validate_json(self.rfile.read(size))
        except (ValueError, ValidationError) as exc:
            self._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"detail": str(exc)})
            return
        result = asyncio.run(SERVICE.dispatch(payload))
        self._json(HTTPStatus.ACCEPTED, result.model_dump(mode="json"))

    def log_message(self, *_: object) -> None:
        return


def _drain_forever() -> None:
    while True:
        asyncio.run(SERVICE.drain_queue())
        threading.Event().wait(1)


def main() -> None:
    threading.Thread(target=_drain_forever, daemon=True, name="sys06-queue-drainer").start()
    ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), AlertHandler).serve_forever()


if __name__ == "__main__":
    main()
