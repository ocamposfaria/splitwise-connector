SELECT
	mlap.month,
	mlap.category,
	mlap.user_name,
	mlap.user_cost as user_limit,
	sum(m.user_cost) as user_cost
FROM
    {{ref('master_limits_and_percentages')}} mlap
JOIN {{ref('master')}} m ON
	m.month = mlap.month
	and m.category = mlap.category
	and m.user_name = mlap.user_name
	and m.group_id in ('33823062', '40055224', '35336773', '34137144')
GROUP BY 1, 2, 3, 4 
ORDER BY 1, 2, 3 DESC 