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

