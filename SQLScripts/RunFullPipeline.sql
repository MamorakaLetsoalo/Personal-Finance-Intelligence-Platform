USE PersonalFinanceIntelligence

-- Incremental Load - Full pipeline orchestration
CREATE OR ALTER PROCEDURE dw.usp_run_full_pipeline
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_id    UNIQUEIDENTIFIER;
    DECLARE @last_load   DATETIME2;
    DECLARE @ins INT = 0, @upd INT = 0, @rej INT = 0;

    -- Start pipeline
    EXEC ctrl.usp_pipeline_start 'full_pipeline', @batch_id OUTPUT;

    BEGIN TRY

        -- Get high watermark
        SELECT @last_load = last_load_ts
        FROM ctrl.etl_pipeline_control
        WHERE pipeline_name = 'full_pipeline';

        -- Load fact table (only new records)
        INSERT INTO dw.fact_financial_events
            (event_id, user_sk, user_id, event_type, amount, interest_rate, event_timestamp, batch_id)
        SELECT
            s.event_id,
            p.user_sk,
            s.user_id,
            UPPER(LTRIM(RTRIM(s.event_type))) AS event_type,
            s.amount,
            s.interest_rate,
            s.event_timestamp,
            @batch_id
        FROM ods.events s
        INNER JOIN dw.dim_user_financial_profile p
            ON s.user_id = p.user_id
           AND p.is_current = 1
        WHERE s.event_timestamp > ISNULL(@last_load, '1900-01-01')
          AND NOT EXISTS (
              SELECT 1
              FROM dw.fact_financial_events t
              WHERE t.event_id = s.event_id
          );

        SET @ins = @@ROWCOUNT;

        -- Refresh dimension
        EXEC dw.usp_merge_dim_user_profile @batch_id;

        -- Recompute metrics
        EXEC dw.usp_compute_financial_metrics @batch_id;

        -- Mark pipeline success ( added NULL for error_message)
        EXEC ctrl.usp_pipeline_finish
            'full_pipeline',
            @batch_id,
            'SUCCESS',
            @ins,
            @upd,
            @rej,
            NULL;

    END TRY
    BEGIN CATCH

        DECLARE @err_msg NVARCHAR(4000);
        SET @err_msg = ERROR_MESSAGE();

        -- Log error
        INSERT INTO ctrl.etl_error_log
            (pipeline_name, batch_id, error_stage, severity, error_message)
        VALUES
            ('full_pipeline', @batch_id, 'ORCHESTRATION', 'CRITICAL', @err_msg);

        -- Mark pipeline failure
        EXEC ctrl.usp_pipeline_finish
            'full_pipeline',
            @batch_id,
            'FAILED',
            @ins,
            @upd,
            @rej,
            @err_msg;

        -- Raise error safely
        RAISERROR (@err_msg, 16, 1);

    END CATCH;

END;
GO