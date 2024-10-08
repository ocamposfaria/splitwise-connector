WITH

years AS (
SELECT cast(generate_series AS string) AS years
FROM generate_series(2022, 2025)
),

months AS (
SELECT LPAD(CAST(generate_series AS TEXT), 2, '0') AS months
FROM generate_series(1, 12)
),

year_months AS (
SELECT
	CONCAT(years, '-', months) AS month,
	years as year
FROM years CROSS JOIN months
WHERE month >= '2022-08'
)

SELECT * FROM year_months