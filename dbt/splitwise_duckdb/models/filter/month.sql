SELECT 
    month
FROM {{ref('master')}} m
GROUP BY 1
ORDER BY 1