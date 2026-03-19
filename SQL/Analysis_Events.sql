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
    -- SELECT * FROM funnel_stages;--
    
-- funnel rates--

SELECT 
	stage_1_views,
    stage_2_cart,
    ROUND(stage_2_cart *100/stage_1_views) AS view_to_cart_rate,
    
    stage_3_checkout,
    ROUND(stage_3_checkout*100/stage_2_cart) AS cart_to_checkout_rate,
    
    stage_4_payment,
    ROUND(stage_4_payment*100/stage_3_checkout) AS checkout_to_payment_rate,
    
	stage_5_purchase,
    ROUND(stage_5_purchase*100/stage_4_payment) AS payment_to_purchase_rate,
    
    ROUND(stage_5_purchase*100/stage_1_views) AS overall_conversion_rate
    
    FROM funnel_stages;
    
    -- Funnel Source --
    
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
            ROUND(carts*100/views) AS cart_conversion_rate,
            ROUND(purchases*100/views) AS purchase_conversion_rate,
            ROUND(purchases*100/carts) AS cart_to_purchase_conversion_rate
            
            FROM source_funnel
            ORDER BY purchases;

-- revenue funnel analysis--
WITH funnel_revenue AS(
	SELECT
		COUNT(DISTINCT CASE WHEN event_type='page_view' THEN user_id END) AS total_visitors,
        COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS total_buyers,
        SUM(CASE WHEN event_type ='purchase' THEN amount END) AS total_revenue
        
	FROM user_events
    WHERE event_date>= TIMESTAMP(DATE_SUB(DATE('2026-03-06'),INTERVAL 90 DAY))
    )
SELECT 
	total_visitors,
    total_buyers,
    total_revenue,
	ROUND(total_buyers/total_visitors,2) AS overall_conversion_rate,
    ROUND(total_revenue/total_buyers,2) AS avg_order_value,
    ROUND(total_revenue/total_visitors,2) AS revenue_per_visitor
    
    FROM funnel_revenue;
-- revenue per channel --
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

-- Revenue per Product --
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
        