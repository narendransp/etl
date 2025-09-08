SELECT 
    d.year,
    d.month,
    SUM(f.total_amount) AS monthly_revenue
FROM orders_df f
JOIN dates_df d  ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
