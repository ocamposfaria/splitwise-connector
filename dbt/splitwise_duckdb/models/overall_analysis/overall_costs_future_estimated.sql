-- presente e futuro estimados nossa residência
WITH averages AS (
SELECT
	cluster,
	category,
	month,
	user_id,
	user_name,
	sum(cost)/6 as cost,
	sum(user_cost)/6 as user_cost
	
FROM
	{{('overall_costs')}}
WHERE
	cluster IN ('nossa residência')
	and month between strftime('%Y-%m',	date_trunc('month',	current_date) - INTERVAL '6 month')
		and strftime('%Y-%m',	date_trunc('month',	current_date) - INTERVAL '1 month')
	
GROUP BY ALL),

year_months AS (SELECT * FROM {{('month')}} WHERE month >= strftime('%Y-%m', date_trunc('month', current_date)))

SELECT 	
	a.cluster,
    a.category,
    ym.month,
    ym.year,
    'previsto' as time_split,
    a.user_id,
    a.user_name,
    a.cost,
    a.cost * mlp.user_percentage as user_cost
FROM averages a
    JOIN {{('master_limits_and_percentages')}} mlp ON mlp.user_name = a.user_name and a.category = mlp.category
    JOIN year_months ym ON mlp.month = ym.month
WHERE mlp.month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')

UNION ALL

-- passado 
SELECT
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
	{{('overall_costs')}}
WHERE
	cluster IN ('nossa residência', 'viagens', 'ganhos', 'compras')
	and month <= strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')

UNION ALL

-- ganhos presente e futuro
SELECT
	'ganhos' as cluster,
	category,
	month,
	substring(month, 1, 4) as year,
	'previsto' as time_split,
	user_id,
	user_name,
	cost,
	user_cost
FROM {{('overall_costs')}}
WHERE
	cluster in ('ganhos')
	and month > strftime('%Y-%m', date_trunc('month', current_date) - INTERVAL '1 month')
	
-- viagens presente e futuro
	
-- compras presente e futuro