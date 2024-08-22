SELECT
	e.group_id,
	g.name AS group_name,
	e.id AS expense_id,
	cast(e.cost as float) as cost,
	-- categoria própria com base em colchetes
	replace(regexp_split_to_array(e.description, '] ')[1], '[', '') AS category,
	regexp_split_to_array(e.description, '] ')[2] AS description,
	-- mês próprio com base nos detalhes separados por quebra de linha
	regexp_split_to_array(e.details, '\n ')[1] AS month,
	-- explodindo a tabela por user_id e expense_id
	unnest(users).user_id AS user_id,
	unnest(users).user.first_name AS user_name,
	unnest(users).owed_share AS user_cost,
	cast(unnest(users).owed_share AS float)/cast(cost as float) AS user_percentage,
	e.details,
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
	e.category,
	e.receipt,
	e.comments,
	g.created_at,
	regexp_split_to_array(e.details, '\n ')[1] = 'old'
FROM
	splitwise.expenses e
FULL OUTER JOIN splitwise.groups g ON
	e.group_id = g.id
WHERE 1=1
	AND e.deleted_at IS NULL
	-- só quero os grupos daqui de casa ou os criados a partir de hoje
	AND (group_id IN (
		'33823062', -- nossa residência
		'40055224', -- apenas lana
		'35336773', -- just me 
	) OR (g.created_at > '2024-08-21'))
	AND replace(regexp_split_to_array(e.description, '] ')[1], '[', '') NOT LIKE '%FILTRAR%'
	AND regexp_split_to_array(e.description, '] ')[2] NOT LIKE '%FILTRAR%'