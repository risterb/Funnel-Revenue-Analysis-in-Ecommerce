-- USER JOURNEY --
-- Ranking events by time--
WITH ordered_events AS (
	SELECT
		user_id,
        event_type,
        event_date,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_date) AS event_order
        FROM user_events
        WHERE event_date >= TIMESTAMP(DATE_SUB(DATE('2026-03-06'), INTERVAL 90 DAY))
),
-- Finding the first view--
first_view AS (
	SELECT
		user_id,
        MIN(event_date) AS view_time
	FROM user_events
    WHERE event_type = 'page_view'
    GROUP BY user_id
    ),
-- Finding fist cart after the view--
first_cart AS (
	SELECT
		fv.user_id,
        MIN(ue.event_date) AS cart_time
	FROM first_view fv
    JOIN user_events ue
		ON fv.user_id = ue.user_id
	WHERE ue.event_type = 'add_to_cart'
	AND ue.event_date > fv.view_time
    GROUP BY fv.user_id
    ),

-- Finding first purchase after cart--
first_purchase AS (
	SELECT
		fc.user_id,
		MIN(ue.event_date) AS purchase_time
	FROM first_cart fc
    JOIN user_events ue
		ON fc.user_id = ue.user_id
	WHERE ue.event_type = 'purchase'
    AND ue.event_date > fc.cart_time
	GROUP BY fc.user_id
    )
    
-- Funnel Metrics--
    SELECT
		DATE_SUB(DATE(fv.view_time), INTERVAL WEEKDAY(DATE(fv.view_time)) DAY) AS week_start,
        COUNT(*) AS converted_users,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE,fv.view_time, fc.cart_time)),2) AS avg_view_to_cart,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE,fc.cart_time, fp.purchase_time)),2) AS avg_cart_to_purchase,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE,fv.view_time, fp.purchase_time)),2) AS avg_total_journey
	FROM first_view fv
    JOIN first_cart fc
		ON fv.user_id=fc.user_id
	JOIN first_purchase fp
		ON fv.user_id = fp.user_id
	GROUP BY week_start
    ORDER BY week_start;
