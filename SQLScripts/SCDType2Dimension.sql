USE PersonalFinanceIntelligence
--Datawarehouse SCD type 2 Dimension
CREATE TABLE dw.dim_user_financial_profile (
    user_sk             INT IDENTITY(1,1) PRIMARY KEY,  -- surrogate key
    user_id             INT             NOT NULL,         -- business / natural key
    age                 INT             NULL,
    salary              DECIMAL(18,2)   NOT NULL,
    salary_bracket      AS (
        CASE
            WHEN salary < 15000  THEN 'Low'
            WHEN salary < 40000  THEN 'Lower-Middle'
            WHEN salary < 80000  THEN 'Middle'
            WHEN salary < 150000 THEN 'Upper-Middle'
            ELSE 'High'
        END
    ) PERSISTED,
    total_debt          DECIMAL(18,2)   NOT NULL DEFAULT 0,
    high_interest_debt  DECIMAL(18,2)   NOT NULL DEFAULT 0,
    low_interest_debt   DECIMAL(18,2)   NOT NULL DEFAULT 0,
    savings_balance     DECIMAL(18,2)   NOT NULL DEFAULT 0,
    monthly_expenses    DECIMAL(18,2)   NULL,
    -- SCD Type 2 columns
    effective_date      DATE            NOT NULL,
    end_date            DATE            NULL,            -- NULL means current
    is_current          BIT             NOT NULL DEFAULT 1,
    -- Audit
    created_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id            UNIQUEIDENTIFIER NOT NULL
);
CREATE INDEX IX_dim_profile_user_current ON dw.dim_user_financial_profile (user_id, is_current);
CREATE INDEX IX_dim_profile_effective    ON dw.dim_user_financial_profile (effective_date, end_date);
GO

-- SCD Type 2 Merge Procedure 
CREATE OR ALTER PROCEDURE dw.usp_merge_dim_user_profile
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;
    BEGIN TRY

        -- Step 1: Expire records where the hash has changed
        UPDATE target
        SET    target.end_date   = CAST(SYSUTCDATETIME() AS DATE),
               target.is_current = 0
        FROM   dw.dim_user_financial_profile AS target
        INNER JOIN (
            SELECT
                u.user_id,
                MAX(u.salary)           AS new_salary,
                SUM(d.balance)          AS new_total_debt,
                SUM(CASE WHEN d.is_high_interest = 1 THEN d.balance ELSE 0 END) AS new_hi_debt,
                SUM(CASE WHEN d.is_high_interest = 0 THEN d.balance ELSE 0 END) AS new_lo_debt
            FROM ods.debt d
            INNER JOIN (SELECT DISTINCT user_id, salary FROM ods.user_financials) u
                ON d.user_id = u.user_id
            WHERE d.is_active = 1
            GROUP BY u.user_id
        ) AS source ON target.user_id = source.user_id
        WHERE target.is_current = 1
          AND (   target.salary          <> source.new_salary
               OR target.total_debt      <> source.new_total_debt
               OR target.high_interest_debt <> source.new_hi_debt);

        -- Insert new current versions
        INSERT INTO dw.dim_user_financial_profile
            (user_id, salary, total_debt, high_interest_debt, low_interest_debt,
             effective_date, end_date, is_current, batch_id)
        SELECT
            s.user_id,
            s.new_salary,
            s.new_total_debt,
            s.new_hi_debt,
            s.new_lo_debt,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1,
            @batch_id
        FROM (
            SELECT
                u.user_id,
                MAX(u.salary)           AS new_salary,
                SUM(d.balance)          AS new_total_debt,
                SUM(CASE WHEN d.is_high_interest = 1 THEN d.balance ELSE 0 END) AS new_hi_debt,
                SUM(CASE WHEN d.is_high_interest = 0 THEN d.balance ELSE 0 END) AS new_lo_debt
            FROM ods.debt d
            INNER JOIN (SELECT DISTINCT user_id, salary FROM ods.user_financials) u
                ON d.user_id = u.user_id
            WHERE d.is_active = 1
            GROUP BY u.user_id
        ) s
        -- Only insert where no current record exists (expired above OR brand new user)
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.dim_user_financial_profile t
            WHERE t.user_id = s.user_id AND t.is_current = 1
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        INSERT INTO ctrl.etl_error_log (pipeline_name, batch_id, error_stage, severity, error_message)
        VALUES ('dim_user_profile', @batch_id, 'LOAD', 'CRITICAL', ERROR_MESSAGE());
        THROW;
    END CATCH;
END;
GO