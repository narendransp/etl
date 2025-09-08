SELECT 
    p.category,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue
FROM orders_df f
JOIN products_df p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 1;
