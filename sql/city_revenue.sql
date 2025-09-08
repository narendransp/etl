SELECT 
    c.city,
    SUM(f.total_amount) AS city_revenue
FROM orders_df f
JOIN customers_df c ON f.customer_id = c.customer_id
GROUP BY c.city
ORDER BY city_revenue DESC;
