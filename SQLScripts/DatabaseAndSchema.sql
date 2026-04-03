-- Enterprise setup: separate schemas per layer
CREATE DATABASE PersonalFinanceIntelligence;
GO

CREATE SCHEMA stg;   -- Raw staging
CREATE SCHEMA ods;   -- Cleaned operational store
CREATE SCHEMA dw;    -- Warehouse (dims + facts)
CREATE SCHEMA ctrl;  -- Control, audit & error tables
CREATE SCHEMA sec;   -- Security views
GO