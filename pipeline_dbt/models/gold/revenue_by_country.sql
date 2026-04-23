SELECT
    country,
    COUNT(order_id) as total_orders,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM pipeline_db.orders_enriched
WHERE status = 'completed'
GROUP BY country
ORDER BY total_revenue DESC