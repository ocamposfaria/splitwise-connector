WITH fundamental_changes AS (
	
	SELECT
		e.group_id,
		CASE 
			WHEN group_id = '39217365' and e.created_at between '2024-09-28' and '2024-10-04' THEN 'Viagem amigos (chapada) 2024'
			WHEN e.id IN ('3289649901') THEN 'Viagem amigos (chapada) 2024'
			ELSE g.name 
		END AS group_name,
		e.id AS expense_id,
		-- categoria própria com base em colchetes
		CASE
		    WHEN e.group_id = '35336773' AND e.description NOT LIKE '%]%' AND e.description NOT LIKE '%[%' THEN 'apenas joão'
		    WHEN e.group_id = '40055224' AND e.description NOT LIKE '%]%' AND e.description NOT LIKE '%[%' THEN 'apenas lana'
		    ELSE 
		        CASE 
		            WHEN array_length(regexp_split_to_array(e.description, '] ')) = 2 
		            	THEN replace(regexp_split_to_array(e.description, '] ')[1], '[', '') 
		            ELSE NULL 
		        END
		END AS category,
		CASE 
			WHEN e.description LIKE '%]%' AND e.description LIKE '%[%' THEN regexp_split_to_array(e.description, '] ')[2]
			ELSE e.description
		END AS description,
		cast(e.cost as float) as cost,
		-- mês próprio com base nos detalhes separados por quebra de linha
		regexp_split_to_array(e.details, '\n ')[1] AS month,
		-- explodindo a tabela por user_id e expense_id
		unnest(users).user_id AS user_id,
		unnest(users).user.first_name AS user_name,
		cast(unnest(users).owed_share AS float) AS user_cost,
		cast(unnest(users).owed_share AS float)/cast(cost AS float) AS user_percentage,
		CASE 
			WHEN cast(unnest(users).paid_share AS float) > 0 THEN True
			WHEN cast(unnest(users).paid_share AS float) = 0 THEN False
			ELSE 'error'
		END AS is_payer,
		regexp_split_to_array(e.details, '\n ')[2] AS details,
		e.repeat_interval,
		e.currency_code,
		e.category_id,
		e.friendship_id,
		e.expense_bundle_id,
		e.repeats,
		e.email_reminder,
		e.email_reminder_in_advance,
		e.next_repeat,
		e.comments_count,
		e.payment,
		e.transaction_confirmed,
		e.repayments,
		e.date,
		e.created_at,
		e.created_by,
		e.updated_at,
		e.updated_by,
		e.deleted_at,
		e.deleted_by,
		e.receipt,
		e.comments
	FROM
		{{ source('splitwise', 'expenses') }} e
	FULL OUTER JOIN {{ source('splitwise', 'groups') }} g ON
		e.group_id = g.id
)

SELECT
	group_id,
	group_name,
	expense_id,
    CASE 
        WHEN array_length(regexp_split_to_array(category, ' - ')) = 2 
        	THEN regexp_split_to_array(category, ' - ')[1]
        ELSE category
	END AS category,
    CASE 
        WHEN array_length(regexp_split_to_array(category, ' - ')) = 2 
        	THEN regexp_split_to_array(category, ' - ')[2]
        ELSE NULL 
	END AS subcategory,
	description,
	cost,
	CASE 
		WHEN month IS NULL THEN substring(created_at, 1, 7)
		ELSE month
	END AS month,
	user_id,
	user_name,
	-- estou colocando o tratamento do grupo da lana aqui pra ser aplicado após o unnest
	CASE
		WHEN group_id = '40055224' AND user_id = '20401164' THEN cost
		WHEN group_id = '40055224' AND user_id = '27512092' THEN 0
		ELSE user_cost
	END AS user_cost,
	CASE
		WHEN group_id = '40055224' AND user_id = '20401164' THEN 1
		WHEN group_id = '40055224' AND user_id = '27512092' THEN 0
		ELSE user_percentage
	END AS user_percentage,
	is_payer,
	details,
	repeat_interval,
	currency_code,
	category_id,
	friendship_id,
	expense_bundle_id,
	repeats,
	email_reminder,
	email_reminder_in_advance,
	next_repeat,
	comments_count,
	payment,
	transaction_confirmed,
	repayments,
	date,
	created_at,
	created_by,
	updated_at,
	updated_by,
	deleted_at,
	deleted_by,
	receipt,
	comments
FROM fundamental_changes
WHERE 1=1 
	-- filtros básicos
	AND description NOT LIKE '%FILTRAR%' 
	AND description <> 'Payment' 
	AND description <> 'QUITE'
	AND deleted_at IS NULL
	-- não quero registros meus vindo do grupo da lana
	AND ((group_id = '40055224' AND user_id = '20401164') OR (group_id <> '40055224'))
	-- remove ganhos (antigos inputs)
	AND (category IS NULL OR category NOT LIKE '%ganhos%')

UNION ALL 

-- ganhos

SELECT

	null as group_id,
	null as group_name,
	null as expense_id,
	gp.category as category,
	null as subcategory,
	gp.description as description,
	null as cost,
	gp.month as month,
	null as user_id,
	gp.user_name as user_name,
	- gp.granular_cost as user_cost,
	null as user_percentage,
	null as is_payer,
	null as details,
	null as repeat_interval,
	null as currency_code,
	null as category_id,
	null as friendship_id,
	null as expense_bundle_id,
	null as repeats,
	null as email_reminder,
	null as email_reminder_in_advance,
	null as next_repeat,
	null as comments_count,
	null as payment,
	null as transaction_confirmed,
	null as repayments,
	null as date,
	null as created_at,
	null as created_by,
	null as updated_at,
	null as updated_by,
	null as deleted_at,
	null as deleted_by,
	null as receipt,
	null as comments

FROM {{ref('gains_and_percentages')}} gp

ORDER BY updated_at DESC
