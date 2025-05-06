SELECT 
    l.category,
    l.limit,
    l.limit_extra_week,
    y.month,
    sum(m.cost) as cost
FROM {{ref('seed_limites')}} l
JOIN {{ref('months')}} y ON y.month between version_month_from and version_month_to
LEFT JOIN {{ref('master')}} m ON m.category = l.category and m.month = y.month
GROUP BY ALL
ORDER BY y.month, cost DESC 