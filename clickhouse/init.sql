CREATE DATABASE IF NOT EXISTS lumen_db;

CREATE TABLE IF NOT EXISTS lumen_db.logs (
    service_name String,
    host String,
    level String,
    message String,
    user_id String,
    timestamp DateTime64(9),
    metadata String
) ENGINE = MergeTree()
ORDER BY timestamp;