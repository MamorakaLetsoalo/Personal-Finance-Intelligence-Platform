# Personal-Finance-Intelligence-Platform

# End to End Data Engineering Project
Microsoft SQL Server · SSIS · Power BI

# Overview

This project delivers a production-grade data platform built on the Microsoft BI stack to track, analyse, and optimise personal financial behaviour over time.

The platform leverages event sourcing to capture financial activities, implements SCD Type 2 to preserve historical financial profiles, and produces actionable insights such as:

Financial Freedom Number
Retirement Targets
Savings Recommendations
Investment Guidance

It is designed to mirror real-world enterprise systems, with a strong focus on scalability, auditability, and data integrity.

# Architecture

The platform is structured across six layers, each implemented in a dedicated SQL Server schema:

Layer
Schema
Purpose
Staging
stg
Raw CSV ingestion with load_date, batch_id, SHA2_256 row hash
ODS
ods
Cleaned, deduplicated, validated data with debt tier classification
Warehouse
dw
SCD Type 2 dimension, event fact, derived metrics fact
Control
ctrl
Pipeline watermarks, batch tracking, error logging
Security
sec
Row-level security — each user sees only their own data
Analytics
Power BI
Five-page dashboard with DirectQuery or Import mode

# Tech Stack
SQL Server 2019+ — Data warehouse & business logic layer
SSIS — ETL orchestration and pipeline automation

![alt text](image-1.png)

![alt text](image-2.png)

![alt text](image-3.png)


Visualisation — Analytical dashboard and reporting

![alt text](image-4.png)

Python 3 — Synthetic data generation
SQL Server Agent — Job scheduling and automation

# Key Features
1. Slowly Changing Dimensions (SCD Type 2)

The dim_user_financial_profile table maintains a complete history of user financial states.

Change detection via SHA2_256 hashing
Automatic versioning:
Old records expire (end_date, is_current = 0)
New records inserted on change
Enables historical analysis and time-aware insights

2. Incremental & Idempotent ETL
Uses watermarks from etl_pipeline_control to process only new data
Prevents duplication via UNIQUE constraints on event_id
Safe reprocessing:
Fact tables use controlled DELETE + INSERT strategy

3. Debt Classification Engine
Tier	Rule
Critical	≥ 15%
High	≥ 10%
Medium	≥ 5%
Low	< 5%

4. Financial Intelligence Engine

Key metrics derived within the warehouse:

Freedom Number = Monthly Expenses × 12 × 25
Retirement Number = Annual Expenses × 25
Net Worth = Savings − Debt
Freedom % = Savings ÷ Freedom Number × 100

Data Quality Framework
Validation rules applied during ODS load
Failed records routed to ods.dq_failures
Quality checks include:
Null user IDs
Negative values
Invalid interest rates
Orphaned records
Downstream processes controlled via dq_pass_flag

Security
Implemented using row-level security (RLS)
Enforced directly in SQL Server
Context-aware filtering via sp_set_session_context
Ensures users can only access their own financial data

# What This Project Demonstrates
Enterprise-grade ETL design
Event-driven data modelling
Historical data tracking (SCD Type 2)
Data quality and governance frameworks
Secure, scalable analytics architecture

Disclaimer: This project is for educational purposes only,it does not constitute financial advise