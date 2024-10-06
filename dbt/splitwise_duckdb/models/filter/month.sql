SELECT 
    month,
    substring(month, 1, 4) as year
FROM {{ref('master')}} m
WHERE group_id in ('33823062', '34137144', '40055224', '35336773')
GROUP BY 1
ORDER BY 1