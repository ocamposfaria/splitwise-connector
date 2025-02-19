SELECT 
    l.category,
    l.limit,
    l.limit_extra_week,
    y.month,
    sum(CAST(m.cost AS DECIMAL)) as cost
FROM {{ref('seed_limites')}} l
JOIN {{ref('months')}} y ON y.month between version_month_from and version_month_to
LEFT JOIN master m ON m.category = l.category and m.month = y.month
GROUP BY ALL
ORDER BY y.month, cost DESC 