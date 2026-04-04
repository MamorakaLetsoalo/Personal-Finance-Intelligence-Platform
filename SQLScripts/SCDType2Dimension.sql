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