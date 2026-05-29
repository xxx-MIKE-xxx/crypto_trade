CREATE TABLE IF NOT EXISTS mints (
    mint TEXT PRIMARY KEY,
    first_seen_ts TEXT NOT NULL,
    last_seen_ts TEXT NOT NULL,
    status TEXT NOT NULL,              -- active, cold, dead_candidate, dead, migrated
    source TEXT NOT NULL,
    last_dex_poll_ts TEXT,
    next_dex_poll_ts TEXT,
    last_security_poll_ts TEXT,
    next_security_poll_ts TEXT,
    priority_score REAL DEFAULT 0,
    dead_reason TEXT,
    migrated_ts TEXT,
    updated_ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dex_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL,
    snapshot_ts TEXT NOT NULL,
    local_received_at_ms INTEGER NOT NULL,
    http_status INTEGER,
    error_type TEXT,
    error_message TEXT,
    data_json TEXT
);

CREATE TABLE IF NOT EXISTS events (
    event_hash TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    mint TEXT,
    event_ts TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ingest_ts TEXT NOT NULL
);