WITH

years AS (
SELECT cast(generate_series AS string) AS years
FROM generate_series(2022, 2025)
),

months AS (
SELECT LPAD(CAST(generate_series AS TEXT), 2, '0') AS months
FROM generate_series(1, 12)
),

year_months AS (
SELECT
	CONCAT(years, '-', months) AS month
FROM years CROSS JOIN months
WHERE month >= '2022-08'
),

five_weeks_months AS (
SELECT '2024-01' AS five_weeks_month UNION ALL
SELECT '2024-04' UNION ALL
SELECT '2024-07' UNION ALL
SELECT '2024-09' 
),

limits_expanded AS (

SELECT
	month,
	category,
	CASE 
		WHEN five_weeks_month IS NOT NULL THEN cost_extra_week
		ELSE cost
	END AS cost,
	cost_juau,
	cost_lana	
FROM year_months ym
LEFT JOIN {{ref('seed_limites')}} sl 
	ON ym.month >= sl.version_month_from and ym.month <= sl.version_month_to
LEFT JOIN five_weeks_months fwm ON ym.month = fwm.five_weeks_month

ORDER BY 1, 2
)

-- unpivot de users (atual)

SELECT
	month,
	category,
	'João' AS user_name,
	cost + cost_juau AS user_cost
FROM limits_expanded
WHERE category <> 'apenas lana'
	AND NOT (month <= '2023-02' and category = 'mercado')

UNION ALL

SELECT
	month,
	category,
	'Hallana' AS user_name,
	cost + cost_lana AS user_cost
FROM limits_expanded
WHERE category <> 'apenas joão'
	AND NOT (month <= '2023-02' and category = 'mercado')

UNION ALL 

-- unpivot de users (para mercado, legado)

SELECT
	month,
	category,
	'João' AS user_name,
	cost_juau AS user_cost
FROM limits_expanded
WHERE category <> 'apenas lana'
	AND (month <= '2023-02' and category = 'mercado')

UNION ALL

SELECT
	month,
	category,
	'Hallana' AS user_name,
	cost_lana AS user_cost
FROM limits_expanded
WHERE category <> 'apenas joão'
	AND (month <= '2023-02' and category = 'mercado')

UNION ALL 

-- presentes

SELECT
	concat(y.years, '-', LPAD(CAST(month AS TEXT), 2, '0')) AS month,
	'presentes' AS category,
	CASE 
		WHEN sp.giver = 'lana' OR sp.giver = 'Hallana' THEN 'Hallana'
		WHEN sp.giver = 'joão' OR sp.giver = 'João' THEN 'João'
	END AS user_name,
	sum(CASE 
		WHEN sp.giver = 'lana' OR sp.giver = 'Hallana' THEN sp.value
		WHEN sp.giver = 'joão' OR sp.giver = 'João' THEN sp.value
	END) AS user_cost
FROM {{ref('seed_presentes')}} sp 
CROSS JOIN years y
GROUP BY 1, 2, 3

ORDER BY 1, 2
