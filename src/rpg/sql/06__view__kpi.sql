CREATE VIEW IF NOT EXISTS view_kpi AS
WITH cte_inventory_room_count AS (

	SELECT
		COUNT(quantity) AS room_count
	FROM
		inventory

),

cte_kpi_source AS (

	SELECT
		hotel_id,
		stay_night AS night_of_stay,
		COUNT(DISTINCT reservation_id) AS occupied_rooms,
		SUM(revenue_net_amount) AS total_net_revenue
	FROM
		view_reservations AS r
	WHERE
		NOT is_cancelled
		AND NOT is_inventory_mismatched
	GROUP BY
		hotel_id, stay_night

)

SELECT
	hotel_id AS HOTEL_ID,
	k.night_of_stay AS NIGHT_OF_STAY,
	ROUND(k.occupied_rooms / c.room_count * 100, 2) AS OCCUPANCY_PERCENTAGE,
	k.total_net_revenue AS TOTAL_NET_REVENUE,
	ROUND(k.total_net_revenue / k.occupied_rooms) AS ADR
FROM
	cte_kpi_source AS k
CROSS JOIN
	cte_inventory_room_count AS c
ORDER BY
	hotel_id, night_of_stay DESC