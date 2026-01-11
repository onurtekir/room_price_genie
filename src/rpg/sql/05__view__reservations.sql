CREATE VIEW IF NOT EXISTS view_reservations AS
WITH cte_most_recent_reservations AS (

	SELECT
		*
	FROM
		reservation_imports
	-- Deduplicate reservations
	QUALIFY ROW_NUMBER() OVER(PARTITION BY hotel_id, reservation_id ORDER BY updated_at DESC, ingested_at DESC) = 1

),

cte_most_recent_stay_dates AS (

	SELECT
		d.*
	FROM
		reservation_stay_dates AS d
	INNER JOIN
		cte_most_recent_reservations AS r
	ON
		r.reservation_hash = d.reservation_hash
	-- Deduplicate stay dates by using reservation_hash + stay_date_hash
	QUALIFY ROW_NUMBER() OVER(PARTITION BY r.reservation_hash, d.stay_date_hash ORDER BY d.ingested_at DESC) = 1

),

cte_reservations AS (

	SELECT
		r.reservation_hash,
		r.hotel_id,
		r.reservation_id,
		r.status,
		r.arrival_date,
		r.departure_date,
		r.source_name,
		r.source_id,
		d.start_date,
		d.end_date,
		CAST(stay_night AS DATE) AS stay_night,
		d.room_type_id,
		d.room_type_name,
		d.number_of_adults,
		d.number_of_children,
		d.revenue_gross_amount,
		d.revenue_net_amount,
		d.fnb_gross_amount,
		d.fnb_net_amount,
		r.created_at,
		r.updated_at,
		r.ingested_at,
		-- For future reporting requirements, add is_inventory_mismatched flag
		IF(i.hotel_id IS NULL, TRUE, FALSE) AS is_inventory_mismatched,
		-- For future reporting requirements, add is_cancelled flag
		IF(LOWER(r.status) = 'cancelled', TRUE, FALSE) AS is_cancelled
	FROM
		cte_most_recent_reservations AS r
	INNER JOIN
		cte_most_recent_stay_dates AS d
	ON
		d.reservation_hash = r.reservation_hash
	LEFT JOIN
		inventory AS i
	ON
		i.hotel_id = r.hotel_id
		AND i.room_type_id = d.room_type_id
	CROSS JOIN
	    -- Generate rows for the nights of grouped stay dates
		GENERATE_SERIES(d.start_date, d.end_date, INTERVAL 1 DAY) AS ds(stay_night)

),

cte_overlapped_nights AS (

	SELECT
		DISTINCT
		hotel_id,
		reservation_id
	FROM
		cte_reservations
	GROUP BY
		hotel_id, reservation_id, stay_night
	HAVING
		COUNT(1) > 1

),

cte_valid_reservations AS (

	SELECT
		*
	FROM
		cte_reservations AS r
	LEFT JOIN
		cte_overlapped_nights AS o
	ON
		o.hotel_id = r.hotel_id
		AND o.reservation_id = r.reservation_id
	WHERE
		o.hotel_id IS NULL  -- Exclude overlapped days

)

SELECT
	*
FROM
	cte_valid_reservations