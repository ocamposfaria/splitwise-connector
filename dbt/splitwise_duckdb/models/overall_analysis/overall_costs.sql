SELECT
	group_id,
	group_name,
	CASE
		-- just me, apenas lana ou nossa residência
		WHEN group_id IN ('35336773', '40055224', '33823062') AND category NOT LIKE '%compras%' THEN 'nossa residência'
		WHEN group_id IN ('999999') THEN 'viagens'-- dummy de viagens
		WHEN group_id IN ('35336773', '40055224', '33823062') AND category LIKE '%compras%' THEN 'compras' 
		WHEN group_id IS NULL THEN 'ganhos'
		ELSE 'unknown'
	END AS cluster,
	category,
	subcategory,
	month,
	user_id,
	user_name,
	sum(cost) AS cost,
	sum(user_cost) AS user_cost
	
FROM {{ref("master")}} m 

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

ORDER BY month DESC 
