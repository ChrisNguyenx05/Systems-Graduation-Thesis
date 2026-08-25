# Phase 04: Deduplication And Rate Limiting

Protect users from alert fatigue.

Initial rules:

- Suppress duplicate alerts for 300 seconds.
- Limit dispatch to 10 messages per minute.
- Log suppressed and delayed alerts.
