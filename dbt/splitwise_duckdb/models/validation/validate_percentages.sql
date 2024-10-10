SELECT
	m.month,
	m.expense_id,
	m.user_id as main_user_id,
	m.user_name,
	m.is_payer,
	m.category,
	m.description,
	m.user_percentage as expense_percentage,
	mlap.user_percentage,
	abs(100*(m.user_percentage - mlap.user_percentage)) as diff,
	ROUND(m.cost, 2) as main_user_paid_share,
	ROUND(m.cost * mlap.user_percentage, 2) as main_user_owed_share,
	CASE
		WHEN m.user_id = '27512092' THEN '20401164'
		WHEN m.user_id = '20401164' THEN '27512092'
	END as second_user_id,
	0 as second_user_paid_share,
	ROUND(ROUND(m.cost, 2) - (ROUND(m.cost * mlap.user_percentage, 2)), 2) as second_user_owed_share,
	CASE 
		WHEN abs(100*(m.user_percentage - mlap.user_percentage)) >= 0.5 THEN True
		ELSE False
	END as should_update
	
FROM
	{{ref("master")}} m
LEFT JOIN {{ref("master_limits_and_percentages")}} mlap ON m.month = mlap.month and m.user_name = mlap.user_name and mlap.category = m.category 
WHERE
	m.group_id = ('33823062')
	and m.category NOT IN ('compras', 'apenas joão', 'apenas lana')
	and m.month between '2023-04' and strftime('%Y-%m', date_trunc('month',	current_date)) and
	CASE 
		WHEN abs(100*(m.user_percentage - mlap.user_percentage)) >= 0.5 THEN True
		ELSE False
	END is True
	and m.expense_id not in (
        '2355272455',
        '2670473359',
        '2842657545',
        '2987075577'
    )
	and is_payer is True