WITH fundamental_changes AS (SELECT
	e.id as expense_id,
	regexp_split_to_array(if(e.details = '', NULL, e.details), '\n ')[1] AS month,
	CASE 
		WHEN array_length(regexp_split_to_array(e.description, '] ')) = 2 
			THEN replace(regexp_split_to_array(e.description, '] ')[1], '[', '') 
		ELSE NULL 
	END AS category,
	CASE 
		WHEN coalesce(e.description, '') LIKE '%]%' AND coalesce(e.description, '') LIKE '%[%' THEN regexp_split_to_array(e.description, '] ')[2]
		ELSE e.description
	END AS description,
	e.cost,
	e.created_at,
	e.updated_at,
	e.details, 
	e.group_id,
	g.name as group_name

FROM
	{{ source('splitwise', 'expenses') }} e
FULL OUTER JOIN {{ source('splitwise', 'groups') }} g ON
	e.group_id = g.id
WHERE g.id = '35336773' -- just me
)

SELECT
	expense_id,
	CASE 
		WHEN month IS NULL THEN substring(created_at, 1, 7)
		ELSE month
	END AS month,
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
	created_at,
	updated_at,
	group_id,
	group_name
FROM fundamental_changes
WHERE month >= '2025-01'
