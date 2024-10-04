SELECT 
    month,
    substring(month, 1, 4) as year
FROM {{ref('master')}} m
GROUP BY 1
ORDER BY 1