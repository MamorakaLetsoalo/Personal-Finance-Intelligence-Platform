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

-- Helper: start a pipeline run(stored procedure)
CREATE OR ALTER PROCEDURE ctrl.usp_pipeline_start
    @pipeline_name NVARCHAR(100),
    @batch_id      UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
    SET @batch_id = NEWID();
    UPDATE ctrl.etl_pipeline_control
    SET status = 'RUNNING', current_run_start = SYSUTCDATETIME(), batch_id = @batch_id,
        rows_extracted = 0, rows_inserted = 0, rows_updated = 0, rows_rejected = 0,
        dq_pass_flag = 1, error_message = NULL, updated_at = SYSUTCDATETIME()
    WHERE pipeline_name = @pipeline_name;

    IF @@ROWCOUNT = 0
        INSERT INTO ctrl.etl_pipeline_control (pipeline_name, batch_id, status, current_run_start)
        VALUES (@pipeline_name, @batch_id, 'RUNNING', SYSUTCDATETIME());
END;
GO

-- Helper: close a pipeline run
CREATE OR ALTER PROCEDURE ctrl.usp_pipeline_finish
    @pipeline_name  NVARCHAR(100),
    @batch_id       UNIQUEIDENTIFIER,
    @status         NVARCHAR(20),
    @rows_inserted  INT = 0,
    @rows_updated   INT = 0,
    @rows_rejected  INT = 0,
    @error_message  NVARCHAR(MAX) = NULL
AS
BEGIN
    UPDATE ctrl.etl_pipeline_control
    SET status          = @status,
        rows_inserted   = @rows_inserted,
        rows_updated    = @rows_updated,
        rows_rejected   = @rows_rejected,
        dq_pass_flag    = CASE WHEN @status = 'SUCCESS' THEN 1 ELSE 0 END,
        error_message   = @error_message,
        last_load_ts    = CASE WHEN @status = 'SUCCESS' THEN SYSUTCDATETIME() ELSE last_load_ts END,
        updated_at      = SYSUTCDATETIME()
    WHERE pipeline_name = @pipeline_name AND batch_id = @batch_id;
END;
GO