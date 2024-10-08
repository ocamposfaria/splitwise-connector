SELECT
	group_id,
	cluster,
	group_name,
	CASE 
		WHEN cluster = 'viagens' THEN lower(group_name)
		WHEN cluster = 'compras' THEN subcategory
		ELSE category
	END as category,
	subcategory,
	month,
	user_id,
	user_name,
	sum(cost) AS cost,
	sum(user_cost) AS user_cost
	
FROM {{ref("master")}} m

WHERE cluster in ('nossa residência', 'compras', 'viagens', 'ganhos')
	and user_id in ('20401164', '27512092')

GROUP BY ALL