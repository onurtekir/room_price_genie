-- Creates inventory table if not exists
CREATE TABLE IF NOT EXISTS rejected_imports (
    rejected_row JSON,
    validation_errors JSON,
    source_filename VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);