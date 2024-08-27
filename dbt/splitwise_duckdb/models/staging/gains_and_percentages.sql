WITH earnings AS (
SELECT
	CASE 
		WHEN is_percentage_eligible LIKE '%sim%' THEN True
		WHEN is_percentage_eligible LIKE '%não%' THEN False
		WHEN is_percentage_eligible LIKE '%nao%' THEN False
	END AS is_percentage_eligible,
	category,
	name AS description,
	CASE 
		WHEN cost_juau = 0 THEN 'Hallana'
		WHEN cost_lana = 0 THEN 'João'
	END AS user_name,
	CASE 
		WHEN cost_juau = 0 THEN cost_lana
		WHEN cost_lana = 0 THEN cost_juau
	END AS user_cost,
	month	
FROM {{ref('seed_ganhos')}} sg
),

total_by_month AS (
SELECT 
	month,
	sum(user_cost) AS cost
FROM earnings
WHERE is_percentage_eligible = True
GROUP BY 1
),

total_by_month_and_user AS (
SELECT 
	month,
	user_name,
	sum(user_cost) AS user_cost
FROM earnings
WHERE is_percentage_eligible = True
GROUP BY 1, 2
),

total_granular AS (
SELECT 
	month,
	is_percentage_eligible,
	category,
	description,
	user_name,
	sum(user_cost) AS user_cost
FROM earnings
GROUP BY 1, 2, 3, 4, 5
)

SELECT
	tg.month,
	tg.is_percentage_eligible,
	tg.category,
	tg.description,
	tg.user_name,
	tg.user_cost AS granular_cost,
	tbmau.user_cost,
	tbm.cost,
	tbmau.user_cost/tbm.cost AS user_percentage
FROM total_granular tg
LEFT JOIN total_by_month tbm ON tg.month = tbm.month
LEFT JOIN total_by_month_and_user tbmau ON tg.month = tbmau.MONTH AND tg.user_name = tbmau.user_name

ORDER BY tg.month ASC, tg.user_name ASC
