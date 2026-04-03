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


