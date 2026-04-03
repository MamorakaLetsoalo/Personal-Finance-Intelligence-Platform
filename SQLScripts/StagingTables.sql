USE PersonalFinanceIntelligence
GO

-- stg.expenses
-- Source: monthly_expenses.csv
-- Columns match the CSV exactly: user_id, first_name, last_name,
--   expense_month, housing, transport, food_groceries, utilities,
--   insurance, entertainment, education, medical, clothing,
--   savings_transfer, total_monthly_expenses

CREATE TABLE stg.expenses (
    stg_id                  INT IDENTITY(1,1)    PRIMARY KEY,
    user_id                 INT                  NOT NULL,
    first_name              NVARCHAR(100)        NULL,
    last_name               NVARCHAR(100)        NULL,
    expense_month           DATE                 NOT NULL,
    housing                 DECIMAL(10,2)        NOT NULL,
    transport               DECIMAL(10,2)        NOT NULL,
    food_groceries          DECIMAL(10,2)        NOT NULL,
    utilities               DECIMAL(10,2)        NOT NULL,
    insurance               DECIMAL(10,2)        NOT NULL,
    entertainment           DECIMAL(10,2)        NOT NULL,
    education               DECIMAL(10,2)        NOT NULL,
    medical                 DECIMAL(10,2)        NOT NULL,
    clothing                DECIMAL(10,2)        NOT NULL,
    savings_transfer        DECIMAL(10,2)        NOT NULL,
    total_monthly_expenses  DECIMAL(10,2)        NOT NULL,

    -- Pipeline metadata
    source_file             NVARCHAR(255)        NULL,
    load_date               DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id                UNIQUEIDENTIFIER     NOT NULL,

    -- Change detection fingerprint 
    row_hash AS CONVERT(
        NVARCHAR(64),
        HASHBYTES(
            'SHA2_256',
            CONVERT(NVARCHAR(20), user_id) + '|' +
            CONVERT(NVARCHAR(10), expense_month, 120) + '|' +
            CONVERT(NVARCHAR(50), total_monthly_expenses)
        ),
    2) PERSISTED
);

-- Index to speed up ODS deduplication lookups
CREATE INDEX IX_stg_expenses_user_month
    ON stg.expenses (user_id, expense_month);
GO

-- stg.savings
-- Source: savings_investments.csv
-- Columns: user_id, first_name, last_name, account_type, institution,
--   current_balance, monthly_contribution, opened_date
CREATE TABLE stg.savings (
    stg_id                  INT IDENTITY(1,1)    PRIMARY KEY,
    user_id                 INT                  NOT NULL,
    first_name              NVARCHAR(100)        NULL,
    last_name               NVARCHAR(100)        NULL,
    account_type            NVARCHAR(100)        NOT NULL,
    institution             NVARCHAR(100)        NOT NULL,
    current_balance         DECIMAL(18,2)        NOT NULL,
    monthly_contribution    DECIMAL(10,2)        NOT NULL,
    opened_date             DATE                 NOT NULL,
    -- Pipeline metadata
    source_file             NVARCHAR(255)        NULL,
    load_date               DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id                UNIQUEIDENTIFIER     NOT NULL,
    -- Change detection fingerprint
    row_hash                AS CONVERT(
                                   NVARCHAR(64),
                                   HASHBYTES(
                                       'SHA2_256',
                                       CONCAT(
                                           user_id,       '|',
                                           account_type,  '|',
                                           institution,   '|',
                                           current_balance, '|',
                                           monthly_contribution
                                       )
                                   ), 2
                               ) PERSISTED
);

CREATE INDEX IX_stg_savings_user_acct
    ON stg.savings (user_id, account_type);
GO

-- stg.salary  
-- Source: salary_updates.csv
CREATE TABLE stg.salary (
    stg_id          INT IDENTITY(1,1)    PRIMARY KEY,
    user_id         INT                  NOT NULL,
    first_name      NVARCHAR(100)        NULL,
    last_name       NVARCHAR(100)        NULL,
    salary_amount   DECIMAL(18,2)        NOT NULL,
    effective_date  DATE                 NOT NULL,
    change_reason   NVARCHAR(200)        NULL,
    -- Pipeline metadata
    source_file     NVARCHAR(255)        NULL,
    load_date       DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id        UNIQUEIDENTIFIER     NOT NULL,
    -- Change detection fingerprint
   row_hash AS CONVERT(
    NVARCHAR(64),
    HASHBYTES(
        'SHA2_256',
        CONVERT(NVARCHAR(20), user_id) + '|' +
        CONVERT(NVARCHAR(50), salary_amount) + '|' +
        CONVERT(NVARCHAR(10), effective_date, 120)  -- yyyy-mm-dd
    ),
2) PERSISTED 
);

CREATE INDEX IX_stg_salary_user_date
    ON stg.salary (user_id, effective_date);
GO


-- stg.debt 
-- Source: debt_records.csv
CREATE TABLE stg.debt (
    stg_id          INT IDENTITY(1,1)    PRIMARY KEY,
    user_id         INT                  NOT NULL,
    first_name      NVARCHAR(100)        NULL,
    last_name       NVARCHAR(100)        NULL,
    debt_type       NVARCHAR(100)        NOT NULL,
    balance         DECIMAL(18,2)        NOT NULL,
    interest_rate   DECIMAL(5,2)         NOT NULL,
    lender          NVARCHAR(100)        NULL,
    start_date      DATE                 NOT NULL,
    is_active       BIT                  NOT NULL DEFAULT 1,
    -- Pipeline metadata
    source_file     NVARCHAR(255)        NULL,
    load_date       DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id        UNIQUEIDENTIFIER     NOT NULL,
    -- Change detection fingerprint
   row_hash AS CONVERT(
    NVARCHAR(64),
    HASHBYTES(
        'SHA2_256',
        CONVERT(NVARCHAR(20), user_id) + '|' +
        CONVERT(NVARCHAR(50), debt_type) + '|' +
        CONVERT(NVARCHAR(50), balance) + '|' +
        CONVERT(NVARCHAR(10), interest_rate) + '|' +
        CONVERT(NVARCHAR(10), start_date, 120)  -- yyyy-mm-dd
    ),
2) PERSISTED
);

CREATE INDEX IX_stg_debt_user_type
    ON stg.debt (user_id, debt_type);
GO


-- stg.events  
-- Source: financial_events.csv
CREATE TABLE stg.events (
    stg_id              INT IDENTITY(1,1)    PRIMARY KEY,
    event_id            NVARCHAR(60)         NOT NULL,   -- business key from CSV
    user_id             INT                  NOT NULL,
    first_name          NVARCHAR(100)        NULL,
    last_name           NVARCHAR(100)        NULL,
    event_type          NVARCHAR(60)         NOT NULL,
    amount              DECIMAL(18,2)        NOT NULL,
    interest_rate       DECIMAL(5,2)         NULL,
    event_timestamp     DATETIME2            NOT NULL,
    -- Pipeline metadata
    source_file         NVARCHAR(255)        NULL,
    load_date           DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id            UNIQUEIDENTIFIER     NOT NULL
    -- No row_hash here: event_id is already a natural unique key
);

CREATE INDEX IX_stg_events_user_ts
    ON stg.events (user_id, event_timestamp);

-- Prevent the same event_id being staged twice in the same batch
CREATE UNIQUE INDEX UQ_stg_events_event_id
    ON stg.events (event_id);
GO
