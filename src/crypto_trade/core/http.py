"""HTTP helpers shared across ingest and feature clients.

Provides a single reusable ``requests.Session`` factory and a small
``get_json`` wrapper that captures status, timing, parse errors, and the
rate-limit response headers we commonly inspect.
"""

