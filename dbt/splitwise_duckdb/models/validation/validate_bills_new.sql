WITH bills AS (
    SELECT
        unnest(['aluguel',
        'condomínio',
        'seguro do carro',
        'energia',
        'internet',
        'IPTU',
        'água',
        'celular lana',
        'gás',
        'seguro incêndio']) as bills
),

bills_by_month AS (
    SELECT 
        *
    FROM bills CROSS JOIN {{ ref('month') }}
),

paid_by_month AS (
    SELECT
        month,
        description
    FROM  {{ ref('master') }} m 
    WHERE 
        is_payer = True
        AND category = 'contas'
        AND {{ filter_bills('description') }}
)

SELECT
    bm.month,
    bm.bills as to_pay
FROM bills_by_month bm 
LEFT JOIN paid_by_month pm ON bm.month = pm.month and pm.description LIKE '%' || bm.bills || '%'
WHERE pm.description IS NULL
ORDER BY 1 DESC 