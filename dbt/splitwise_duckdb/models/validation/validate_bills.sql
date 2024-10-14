{{ config(materialized='view') }}

WITH bills AS (
    SELECT 
        month, description
    FROM
        {{ref('master')}}
    WHERE 1=1 
        and is_payer = True
        and category = 'contas'
        and {{ filter_bills('description') }} 
)

SELECT 
	month, 
    count(description) as count, 
    string_agg("description", ', ' ORDER BY "description" DESC) as bills
FROM bills 
GROUP BY 1
ORDER BY 1 DESC 