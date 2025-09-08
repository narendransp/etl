SELECT 
    c.customer_id,
    c.customer_name,
    SUM(f.total_amount) AS total_spend
FROM orders_df f
JOIN customers_df c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spend DESC
LIMIT 5;
