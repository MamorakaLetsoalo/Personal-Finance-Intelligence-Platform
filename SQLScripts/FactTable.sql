USE PersonalFinanceIntelligence

-- Incremental-load-safe fact table
CREATE TABLE dw.fact_financial_events (
    event_sk            INT IDENTITY(1,1) PRIMARY KEY,
    event_id            NVARCHAR(50)    NOT NULL,   -- business key (for idempotency)
    user_sk             INT             NOT NULL REFERENCES dw.dim_user_financial_profile(user_sk),
    user_id             INT             NOT NULL,   -- degenerate dim (for late-arriving joins)
    event_type          NVARCHAR(50)    NOT NULL,
    amount              DECIMAL(18,2)   NOT NULL,
    interest_rate       DECIMAL(5,2)    NULL,
    event_timestamp     DATETIME2       NOT NULL,
    load_date           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id            UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT UQ_event_business_key UNIQUE (event_id)  -- prevents duplicate loads
);
CREATE INDEX IX_fact_events_user_ts ON dw.fact_financial_events (user_id, event_timestamp);
CREATE INDEX IX_fact_events_type    ON dw.fact_financial_events (event_type, event_timestamp);

-- Derived metrics fact (refreshed by stored proc each run)
CREATE TABLE dw.fact_financial_metrics (
    metric_sk               INT IDENTITY(1,1) PRIMARY KEY,
    user_sk                 INT             NOT NULL REFERENCES dw.dim_user_financial_profile(user_sk),
    user_id                 INT             NOT NULL,
    snapshot_date           DATE            NOT NULL DEFAULT CAST(SYSUTCDATETIME() AS DATE),
    -- Core calculations
    freedom_number          DECIMAL(18,2)   NULL,
    retirement_number       DECIMAL(18,2)   NULL,
    monthly_savings_target  DECIMAL(18,2)   NULL,
    debt_free_months        INT             NULL,
    net_worth               DECIMAL(18,2)   NULL,
    -- Profiles
    risk_profile            NVARCHAR(20)    NULL,  -- Conservative|Balanced|Growth
    investment_strategy     NVARCHAR(30)    NULL,
    -- Progress
    freedom_pct             AS (
        CASE WHEN freedom_number > 0
             THEN CAST(savings_balance / freedom_number * 100 AS DECIMAL(5,2))
             ELSE 0 END
    ) PERSISTED,
    savings_balance         DECIMAL(18,2)   NULL,  -- snapshot from dim
    batch_id                UNIQUEIDENTIFIER NOT NULL,
    created_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
