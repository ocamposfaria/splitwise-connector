WITH

years AS (
SELECT cast(generate_series AS string) AS years
FROM generate_series(2025, 2026)
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
)

SELECT * FROM year_months