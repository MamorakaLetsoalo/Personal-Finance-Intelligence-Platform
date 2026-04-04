--Create ODS layer (Operational data store)
--intergrates data from multiple sources
USE PersonalFinanceIntelligence

CREATE TABLE ods.debt (
    ods_debt_id         INT IDENTITY(1,1) PRIMARY KEY,
    user_id             INT             NOT NULL,
    debt_type           NVARCHAR(50)    NOT NULL,
    balance             DECIMAL(18,2)   NOT NULL,
    interest_rate       DECIMAL(5,2)    NOT NULL,
    interest_tier       AS (
        CASE
            WHEN interest_rate >= 15 THEN 'Critical'
            WHEN interest_rate >= 10 THEN 'High'
            WHEN interest_rate >= 5  THEN 'Medium'
            ELSE 'Low'
        END
    ) PERSISTED,
    is_high_interest    AS (CASE WHEN interest_rate >= 10 THEN 1 ELSE 0 END) PERSISTED,
    lender              NVARCHAR(100)   NULL,
    effective_date      DATE            NOT NULL,
    is_active           BIT             NOT NULL DEFAULT 1,
    ods_load_date       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id            UNIQUEIDENTIFIER NOT NULL
);

-- Data quality log (one row per failed rule per batch)
CREATE TABLE ods.dq_failures (
    dq_id           INT IDENTITY(1,1) PRIMARY KEY,
    table_name      NVARCHAR(100)   NOT NULL,
    rule_name       NVARCHAR(100)   NOT NULL,
    rule_description NVARCHAR(500)  NOT NULL,
    failed_count    INT             NOT NULL,
    batch_id        UNIQUEIDENTIFIER NOT NULL,
    checked_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO