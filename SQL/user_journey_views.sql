CREATE VIEW funnel_rate AS
WITH funnel_stages AS (
SELECT
	COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS stage_1_views,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS stage_2_cart,
    COUNT(DISTINCT CASE WHEN event_type = 'checkout_start' THEN user_id END) AS stage_3_checkout,
    COUNT(DISTINCT CASE WHEN event_type = 'payment_info' THEN user_id END) AS stage_4_payment,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS stage_5_purchase
    FROM user_events
    
    WHERE event_date >= TIMESTAMP(DATE_SUB(DATE('2026-03-06'),INTERVAL 90 DAY))
    )

SELECT 'Page views' AS stage, stage_1_views AS users, NULL AS conversion_rate FROM funnel_stages
UNION ALL
SELECT 'Add to cart', stage_2_cart, ROUND(stage_2_cart/stage_1_views,2) FROM funnel_stages
UNION ALL
SELECT 'Checkout',stage_3_checkout, ROUND(stage_3_checkout/stage_2_cart,2) FROM funnel_stages
UNION ALL
SELECT 'Payment Info', stage_4_payment, ROUND(stage_4_payment/stage_3_checkout,2) FROM funnel_stages
UNION ALL
SELECT 'Purchase', stage_5_purchase, ROUND(stage_5_purchase/stage_4_payment,2) FROM funnel_stages;

SELECT * FROM funnel_rate;

-- ------------------------------------------------
CREATE VIEW funnel_by_source AS
    WITH source_funnel AS (
		SELECT
        traffic_source,
			COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS views,
            COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS carts,
            COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchases
            
            FROM user_events
            
            WHERE event_date >= TIMESTAMP(DATE_SUB(DATE('2026-03-06'),INTERVAL 90 DAY))
            
            GROUP BY traffic_source
		)
        
        SELECT 
			traffic_source,
            views,
            carts,
            purchases,
            ROUND(carts/views,2) AS cart_conversion_rate,
            ROUND(purchases/views,2) AS purchase_conversion_rate,
            ROUND(purchases/carts,2) AS cart_to_purchase_conversion_rate
            
            FROM source_funnel
            ORDER BY purchases;

-- --------------------------------------------------------

CREATE VIEW  user_journey AS
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

    SELECT * FROM user_journey;
    
-- ---------------------------------------------
CREATE VIEW channel_revenue AS
WITH channel_revenue AS (
	SELECT 
		traffic_source,
        COUNT(DISTINCT CASE WHEN event_type='page_view' THEN user_id END) AS total_visitors,
        COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS total_buyers,
        SUM(CASE WHEN event_type='purchase' THEN amount END) AS total_revenue
	FROM user_events
    WHERE event_date >= TIMESTAMP(DATE_SUB(DATE('2026-03-06'), INTERVAL 90 DAY))
    GROUP BY traffic_source)
SELECT 
	traffic_source,
    total_visitors,
    total_buyers,
    total_revenue,
    ROUND((total_buyers/total_visitors),2) AS overall_conversion_rate,
    ROUND((total_revenue/total_buyers),2) AS avg_order_value,
    ROUND((total_revenue/total_visitors),2) AS revenue_per_visitor
FROM channel_revenue
ORDER BY total_revenue DESC;

-- -------------------------------------------
CREATE VIEW product_revenue AS 
WITH product_revenue AS (
	SELECT
		product_id,
        COUNT(DISTINCT CASE WHEN event_type='page_view' THEN user_id END) AS product_viewers,
        COUNT(DISTINCT CASE WHEN event_type='add_to_cart' THEN user_id END) AS cart_adds,
        COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS total_buyers,
        SUM(CASE WHEN event_type='purchase' THEN amount END) AS total_revenue
	FROM user_events
    WHERE event_date >= TIMESTAMP(DATE_SUB(DATE('2026-03-06'), INTERVAL 90 DAY))
    GROUP BY product_id)
    SELECT
		product_id,
        product_viewers,
        cart_adds,
        total_buyers,
        total_revenue,
        ROUND((cart_adds/NULLIF(product_viewers,0)),2) AS view_to_cart_rate,
        ROUND((total_buyers/NULLIF(cart_adds,0)),2) AS cart_to_purchase_rate,
        ROUND((total_revenue/NULLIF(total_buyers,0)),2) AS avg_order_value,
        ROUND((total_revenue/NULLIF(product_viewers,0)),2) AS renevue_per_viewer
	FROM product_revenue;
