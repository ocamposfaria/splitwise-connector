SELECT
	*
FROM
	{{ source('splitwise', 'expenses') }} e
FULL OUTER JOIN {{ source('splitwise', 'groups') }} g ON
	e.group_id = g.id
