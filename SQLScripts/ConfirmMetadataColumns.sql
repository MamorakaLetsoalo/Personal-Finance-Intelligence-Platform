use PersonalFinanceIntelligence 
-- Confirm all metadata columns are populated across all five tables
SELECT 'stg.salary' AS tbl,
    COUNT(*)                                    AS total_rows,
    SUM(CASE WHEN batch_id  IS NULL THEN 1 ELSE 0 END) AS missing_batch_id,
    SUM(CASE WHEN load_date IS NULL THEN 1 ELSE 0 END) AS missing_load_date,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS missing_source_file,
    SUM(CASE WHEN row_hash IS NULL THEN 1 ELSE 0 END)  AS missing_row_hash
FROM stg.salary

UNION ALL

SELECT 'stg.debt',
    COUNT(*),
    SUM(CASE WHEN batch_id    IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN load_date   IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_hash    IS NULL THEN 1 ELSE 0 END)
FROM stg.debt

UNION ALL

SELECT 'stg.expenses',
    COUNT(*),
    SUM(CASE WHEN batch_id    IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN load_date   IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_hash    IS NULL THEN 1 ELSE 0 END)
FROM stg.expenses

UNION ALL

SELECT 'stg.savings',
    COUNT(*),
    SUM(CASE WHEN batch_id    IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN load_date   IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN row_hash    IS NULL THEN 1 ELSE 0 END)
FROM stg.savings

UNION ALL

SELECT 'stg.events',
    COUNT(*),
    SUM(CASE WHEN batch_id    IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN load_date   IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END),
    0 AS missing_row_hash   -- stg.events has no row_hash by design
FROM stg.events;