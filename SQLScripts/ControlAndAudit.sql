USE PersonalFinanceIntelligence
GO


-- Master pipeline control table
CREATE TABLE ctrl.etl_pipeline_control (
    pipeline_id        INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name      NVARCHAR(100)   NOT NULL,
    batch_id           UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    last_load_ts       DATETIME2        NULL,
    current_run_start  DATETIME2        NULL,
    rows_extracted     INT              NULL,
    rows_inserted      INT              NULL,
    rows_updated       INT              NULL,
    rows_rejected      INT              NULL,
    dq_pass_flag       BIT              NOT NULL DEFAULT 1,
    status             NVARCHAR(20)     NOT NULL DEFAULT 'PENDING',  -- PENDING|RUNNING|SUCCESS|FAILED
    error_message      NVARCHAR(MAX)    NULL,
    created_at         DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Granular error log
CREATE TABLE ctrl.etl_error_log (
    error_id        INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name   NVARCHAR(100)   NOT NULL,
    batch_id        UNIQUEIDENTIFIER NOT NULL,
    error_stage     NVARCHAR(50)    NOT NULL,   -- EXTRACT|STAGE|TRANSFORM|LOAD
    severity        NVARCHAR(10)    NOT NULL,   -- INFO|WARN|ERROR|CRITICAL
    source_table    NVARCHAR(100)   NULL,
    record_key      NVARCHAR(200)   NULL,       -- e.g. user_id + event_id
    error_message   NVARCHAR(MAX)   NOT NULL,
    error_ts        DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
