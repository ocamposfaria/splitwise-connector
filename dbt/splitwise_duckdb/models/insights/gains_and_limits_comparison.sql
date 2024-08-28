-- essa consulta é útil para validar quantos % do que ganhamos está fora dos limites básicos

WITH 

summed_up_gains AS (
SELECT
	month,
	user_name,
	SUM(user_cost) AS user_cost
FROM
	{{ref('master')}} m
WHERE category = 'ganhos' AND category <> 'ganhos extra'
GROUP BY 1, 2 
),

summed_up_limits AS (
SELECT
	mlap.month,
	mlap.user_name,
	sum(mlap.user_cost) AS user_cost
FROM {{ref('master_limits_and_percentages')}} mlap 
GROUP BY 1, 2
)

SELECT
	sug.month,
	sug.user_name,
	sum(sug.user_cost) AS user_gains,
	sum(sul.user_cost) AS user_limit,
	sum(sug.user_cost) - (sum(sul.user_cost)) AS diff,
	(sum(sug.user_cost) - (sum(sul.user_cost)))/(sum(sug.user_cost)) AS ratio
	
FROM summed_up_gains sug 
LEFT JOIN summed_up_limits sul ON sug.month = sul.month AND sug.user_name = sul.user_name 
GROUP BY 1, 2
ORDER BY 1, 2
