WITH 

summed_up_percentages AS (
SELECT
	month,
	user_name,
	FIRST(user_percentage) AS user_percentage
FROM {{ref('gains_and_percentages')}} gap
GROUP BY 1, 2
)

SELECT
	l.month,
	l.category,
	l.user_name,
	CASE 
		WHEN l.category = 'apenas lana' OR l.category = 'apenas joão' THEN l.user_cost 
		ELSE l.user_cost * sup.user_percentage
	END AS user_cost,
	CASE 
		WHEN l.category = 'apenas lana' OR l.category = 'apenas joão' THEN 1 
		ELSE sup.user_percentage
	END AS user_percentage
FROM
	{{ref('limits')}} l 
LEFT JOIN summed_up_percentages sup ON l.month = sup.MONTH AND l.user_name = sup.user_name
WHERE l.month >= '2022-08'
ORDER BY 1, 2
