SELECT
	group_id,
	group_name,
	category,
	subcategory,
	month,
	user_id,
	user_name,
	avg(user_percentage) AS user_percentage,
	sum(cost) AS cost,
	sum(user_cost) AS user_cost
	
FROM {{ref('master')}} m 

GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY month DESC 
