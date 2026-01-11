-- Creates inventory table if not exists
CREATE TABLE IF NOT EXISTS reservation_stay_dates (
    hotel_id INTEGER,
    reservation_id VARCHAR,
    start_date DATE,
    end_date DATE,
    room_type_id VARCHAR,
    room_type_name VARCHAR,
    number_of_adults INTEGER,
    number_of_children INTEGER,
    revenue_gross_amount DECIMAL(18, 2),
    revenue_net_amount DECIMAL(18, 2),
    fnb_gross_amount DECIMAL(18, 2),
    fnb_net_amount DECIMAL(18, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    reservation_hash VARCHAR,
    stay_date_hash VARCHAR
);