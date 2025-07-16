WITH rotina_ganhos_viagens AS (
    SELECT
        month,
        CASE
            WHEN group_name = 'just me' THEN 'rotina'
            WHEN group_name is NULL THEN 'ganhos'
            WHEN group_name in ('lollapalooza 2025') THEN 'viagens' -- viagens aqui!
        END as segmentation,
        CASE
            WHEN group_name in ('lollapalooza 2025') THEN group_name -- viagens aqui!
            ELSE if(category LIKE '%rol%', 'rolês', category)
        END as category,
        sum(cost) as cost
    FROM {{ref('master')}}
    WHERE category <> 'compras'
    GROUP BY 1, 2, 3
),

compras AS (
    SELECT
        month,
        'compras' as segmentation,
        subcategory as category,
        cost
    FROM {{ref('master')}}
    WHERE category in ('compras')
),

compras_mean AS (
SELECT
	'compras' as segmentation,
	SUM(cost)/3 as cost
FROM
	{{ref('master')}}
WHERE
	category in ('compras')
	and month >= SUBSTRING(CAST(current_date() - INTERVAL 3 month AS STRING), 1, 7)
GROUP BY 1),

passado AS (
    SELECT * FROM rotina_ganhos_viagens 
    WHERE month < SUBSTRING(CAST(current_date() AS STRING), 1, 7)
    UNION ALL 
    SELECT * FROM compras
),

means AS (
    SELECT 
        'rotina' as segmentation,
        if(category LIKE '%rol%', 'rolês', category) as category,
        sum(cost)/3 as cost
    FROM master_limits
    WHERE month >= SUBSTRING(CAST(current_date() - INTERVAL 3 month AS STRING), 1, 7)
        and month < SUBSTRING(CAST(current_date() AS STRING), 1, 7)
        -- últimos 6 meses fechados
    GROUP BY 1, 2),

presente_futuro AS (
    SELECT 
        month, 
        'rotina' as segmentation,
        if(category LIKE '%rol%', 'rolês', category) as category,
        cost 
    FROM means CROSS JOIN months
    WHERE month >= SUBSTRING(CAST(current_date() AS STRING), 1, 7)
    UNION ALL
    SELECT
        month,
        'ganhos' as segmentation,
        if(category LIKE '%rol%', 'rolês', category) as category,
        cost
    FROM {{ref('master')}}
    WHERE category in ('ganhos', 'ganhos extra')
    AND month >= SUBSTRING(CAST(current_date() AS STRING), 1, 7)
    UNION ALL 
    SELECT
        month, 
        segmentation,
        'compras' as category,
        cost
    FROM
        compras_mean cm
    CROSS JOIN months m
    WHERE month >= SUBSTRING(CAST(current_date() AS STRING), 1, 7)
),

tudo_menos_poupança AS (
SELECT * FROM passado
UNION ALL 
SELECT * FROM presente_futuro
),

poupança AS (
    SELECT
        month,
        'poupança' as segmentation,
        'poupança' as category,
        - sum(cost) as cost
    FROM tudo_menos_poupança
    GROUP BY 1, 2, 3
)

SELECT *, 'estimado' as time_split FROM tudo_menos_poupança
WHERE month >= SUBSTRING(CAST(current_date() AS STRING), 1, 7)
UNION ALL 
SELECT *, 'estimado' as time_split FROM poupança
WHERE month >= SUBSTRING(CAST(current_date() AS STRING), 1, 7)
UNION ALL
SELECT *, 'realizado' as time_split FROM tudo_menos_poupança
WHERE month < SUBSTRING(CAST(current_date() AS STRING), 1, 7)
UNION ALL 
SELECT *, 'realizado' as time_split FROM poupança
WHERE month < SUBSTRING(CAST(current_date() AS STRING), 1, 7)
