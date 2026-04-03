USE PersonalFinanceIntelligence
GO

CREATE TABLE stg.salary (
    stg_id          INT IDENTITY(1,1) PRIMARY KEY,
    user_id         INT             NOT NULL,
    salary_amount   DECIMAL(18,2)   NOT NULL,
    effective_date  DATE            NOT NULL,
    source_file     NVARCHAR(255)   NULL,
    load_date       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id        UNIQUEIDENTIFIER NOT NULL,

    row_hash AS CONVERT(NVARCHAR(64),
        HASHBYTES(
            'SHA2_256',
            CONVERT(NVARCHAR(20), user_id) + '|' +
            CONVERT(NVARCHAR(50), salary_amount) + '|' +
            CONVERT(NVARCHAR(10), effective_date, 120)  -- ISO format yyyy-mm-dd
        ),
    2) PERSISTED
);

CREATE TABLE stg.debt (
    stg_id          INT IDENTITY(1,1) PRIMARY KEY,
    user_id         INT             NOT NULL,
    debt_type       NVARCHAR(50)    NOT NULL,
    balance         DECIMAL(18,2)   NOT NULL,
    interest_rate   DECIMAL(5,2)    NOT NULL,
    lender          NVARCHAR(100)   NULL,
    effective_date  DATE            NOT NULL,
    source_file     NVARCHAR(255)   NULL,
    load_date       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id        UNIQUEIDENTIFIER NOT NULL,
    row_hash        AS CONVERT(NVARCHAR(64),
                       HASHBYTES('SHA2_256', CONCAT(user_id,'|',debt_type,'|',balance,'|',interest_rate))
                       ,2) PERSISTED
);