SELECT
    product_name,
    category,
    COUNT(order_id) as times_ordered,
    SUM(quantity) as total_units_sold,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM pipeline_db.orders_enriched
GROUP BY product_name, category
ORDER BY total_revenue DESC