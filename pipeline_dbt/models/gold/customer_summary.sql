SELECT
    customer_id,
    customer_name,
    email,
    city,
    country,
    COUNT(order_id) as total_orders,
    ROUND(SUM(total_amount), 2) as lifetime_value,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order
FROM pipeline_db.orders_enriched
GROUP BY customer_id, customer_name, email, city, country
ORDER BY lifetime_value DESC