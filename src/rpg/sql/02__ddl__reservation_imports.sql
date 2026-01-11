-- Creates inventory table if not exists
CREATE TABLE IF NOT EXISTS reservation_imports (
    hotel_id INTEGER,
    reservation_id VARCHAR,
    status VARCHAR,
    arrival_date DATE,
    departure_date DATE,
    source_name VARCHAR,
    source_id VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_filename VARCHAR,
    ingested_at TIMESTAMP DEFAULT now(),
    reservation_hash VARCHAR
);