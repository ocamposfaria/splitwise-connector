-- presente e futuro estimados nossa residência
WITH tmp_averages AS (
SELECT
	cluster,
	category,
	month,
	user_id,
	user_name,
	sum(cost)/6 as cost,
	sum(user_cost)/6 as user_cost
	
FROM
	{{ref("overall_costs")}}
WHERE
	cluster IN ('nossa residência')
	and month between strftime('%Y-%m',	date_trunc('month',	current_date) - INTERVAL '6 month')
		and strftime('%Y-%m', date_trunc('month',	current_date) - INTERVAL '1 month')
	
GROUP BY ALL),

year_months AS (SELECT * FROM {{ref("month")}} WHERE month >= strftime('%Y-%m', date_trunc('month', current_date))),

future_trips_and_shopping AS (
	SELECT 
		cluster,
		category,
		ym.month,
		ym.year,
		'previsto' as time_split,
		user_id,
		user_name,
		cost,
		user_cost

	FROM
		{{ref("overall_costs")}} oc
	JOIN year_months ym ON ym.month = oc.month
	WHERE 
		cluster in ('viagens', 'compras')
),

future_spends AS (SELECT 	
	a.cluster,
    a.category,
    ym.month,
    ym.year,
    'previsto' as time_split,
    a.user_id,
    a.user_name,
    a.cost,
    a.cost * mlp.user_percentage as user_cost
FROM tmp_averages a
    JOIN {{ref("master_limits_and_percentages")}} mlp ON mlp.user_name = a.user_name and a.category = mlp.category
    JOIN year_months ym ON mlp.month = ym.month
WHERE mlp.month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')),

-- passado com ganhos extra
past_spends AS (SELECT
	cluster,
    category,
    month,
    substring(month, 1, 4) as year,
    'realizado' as time_split,
    user_id,
    user_name,
    cost,
    user_cost
FROM
	{{ref("overall_costs")}}
WHERE
	cluster IN ('nossa residência', 'viagens', 'ganhos', 'compras')
	and month <= strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')),

-- passado sem ganhos extra
past_spends_wo_extra AS (SELECT
	cluster,
    category,
    month,
    substring(month, 1, 4) as year,
    'realizado' as time_split,
    user_id,
    user_name,
    cost,
    user_cost
FROM
	{{ref("overall_costs")}}
WHERE
	cluster IN ('nossa residência', 'viagens', 'ganhos', 'compras')
    and coalesce(category, '') NOT LIKE '%ganhos extra%'
	and month <= strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')),

-- ganhos com ganhos extra presente e futuro
gains AS (SELECT
	'ganhos' as cluster,
	category,
	month,
	substring(month, 1, 4) as year,
	'previsto' as time_split,
	user_id,
	user_name,
	cost,
	user_cost
FROM {{ref("overall_costs")}}
WHERE
	cluster in ('ganhos')
	and month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')),

-- ganhos sem ganhos extra presente e futuro
gains_wo_extra AS (SELECT
	'ganhos' as cluster,
	category,
	month,
	substring(month, 1, 4) as year,
	'previsto' as time_split,
	user_id,
	user_name,
	cost,
	user_cost
FROM {{ref("overall_costs")}}
WHERE
	cluster in ('ganhos')
    and coalesce(category, '') NOT LIKE ('%ganhos extra%')
	and month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')),

planned_for_future AS (
    SELECT
        "group" as cluster,
        category,
        month,
        substring(month, 1, 4) as year,
        'previsto' as time_split,
        null as user_id,
        'João' as user_name,
        cost_juau + cost_lana as cost,
        cost_juau as user_cost
    FROM {{ref("seed_gastos_futuros")}}
    WHERE  month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')
    UNION ALL
    SELECT
        "group" as cluster,
        category,
        month,
        substring(month, 1, 4) as year,
        'previsto' as time_split,
        null as user_id,
        'Hallana' as user_name,
        cost_juau + cost_lana as cost,
        cost_lana as user_cost
    FROM {{ref("seed_gastos_futuros")}}
    WHERE  month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')
),

totals AS (SELECT * FROM future_spends
UNION ALL
SELECT * FROM future_trips_and_shopping
UNION ALL
SELECT * FROM planned_for_future
UNION ALL
SELECT * FROM past_spends
UNION ALL
SELECT * FROM gains),

totals_wo_extra AS (SELECT * FROM future_spends
UNION ALL
SELECT * FROM future_trips_and_shopping
UNION ALL
SELECT * FROM planned_for_future
UNION ALL
SELECT * FROM past_spends_wo_extra
UNION ALL
SELECT * FROM gains),

savings AS (
SELECT 
	'poupança' as cluster,
	'poupança' as category,
	month,
	year,
	time_split,
	user_id,
	user_name,
	- sum(cost) as cost,
	- sum(user_cost) as user_cost
FROM totals
GROUP BY ALL
),

savings_wo_extra AS (
SELECT 
	'poupança' as cluster,
	'poupança' as category,
	month,
	year,
	time_split,
	user_id,
	user_name,
	- sum(cost) as cost,
	- sum(user_cost) as user_cost
FROM totals_wo_extra
GROUP BY ALL
)

SELECT *, 'ganhos + ganhos extra' as earning_category FROM totals
UNION ALL
SELECT *, 'ganhos + ganhos extra' as earning_category FROM savings

UNION ALL 

SELECT *, 'ganhos' as earning_category FROM totals_wo_extra
UNION ALL
SELECT *, 'ganhos' as earning_category FROM savings_wo_extra