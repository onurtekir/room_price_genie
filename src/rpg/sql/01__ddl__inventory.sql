-- Creates inventory table if not exists
CREATE TABLE IF NOT EXISTS inventory (
    hotel_id VARCHAR,
    room_type_id VARCHAR,
    quantity INTEGER,
    source_filename VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    ingested_at TIMESTAMP
);