import pandas as pd
import pyodbc
import uuid
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────────────────
SERVER   = r'DESKTOP-QI6H2EA'
DATABASE = 'PersonalFinanceIntelligence'
DATA_DIR = r'C:\Users\Admin\Desktop\PF_Platform\data'

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

def get_conn():
    return pyodbc.connect(CONN_STR)

def now():
    return datetime.now(timezone.utc)

# ── START ────────────────────────────────────────────────────────────────
batch_id  = str(uuid.uuid4())
load_date = now()

print(f"\nBatch ID : {batch_id}")
print(f"Load time: {load_date}\n")

conn = get_conn()

# ── 1. stg.salary ────────────────────────────────────────────────────────
print('Loading stg.salary ...')

df = pd.read_csv(f'{DATA_DIR}\\salary_updates.csv')
df.columns = df.columns.str.strip()

cursor = conn.cursor()
cursor.fast_executemany = True

cursor.execute('TRUNCATE TABLE stg.salary')
conn.commit()

rows = [
    (
        int(r.user_id),
        str(r.first_name),
        str(r.last_name),
        float(r.salary_amount),
        str(r.effective_date),
        str(r.change_reason),
        f'{DATA_DIR}\\salary_updates.csv',
        load_date,
        batch_id
    )
    for r in df.itertuples(index=False)
]

cursor.executemany("""
    INSERT INTO stg.salary
    (user_id, first_name, last_name, salary_amount,
     effective_date, change_reason, source_file, load_date, batch_id)
    VALUES (?,?,?,?,?,?,?,?,?)
""", rows)

conn.commit()
cursor.close()

print(f'  stg.salary: {len(rows)} rows loaded')

# ── 2. stg.debt ──────────────────────────────────────────────────────────
print('Loading stg.debt ...')

df = pd.read_csv(f'{DATA_DIR}\\debt_records.csv')
df.columns = df.columns.str.strip()

cursor = conn.cursor()
cursor.fast_executemany = True

cursor.execute('TRUNCATE TABLE stg.debt')
conn.commit()

rows = [
    (
        int(r.user_id),
        str(r.first_name),
        str(r.last_name),
        str(r.debt_type),
        float(r.balance),
        float(r.interest_rate),
        str(r.lender),
        str(r.start_date),
        int(r.is_active),
        f'{DATA_DIR}\\debt_records.csv',
        load_date,
        batch_id
    )
    for r in df.itertuples(index=False)
]

cursor.executemany("""
    INSERT INTO stg.debt
    (user_id, first_name, last_name, debt_type, balance,
     interest_rate, lender, start_date, is_active,
     source_file, load_date, batch_id)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
""", rows)

conn.commit()
cursor.close()

print(f'  stg.debt: {len(rows)} rows loaded')

# ── 3. stg.expenses ──────────────────────────────────────────────────────
print('Loading stg.expenses ...')

df = pd.read_csv(f'{DATA_DIR}\\monthly_expenses.csv')
df.columns = df.columns.str.strip()

cursor = conn.cursor()
cursor.fast_executemany = True

cursor.execute('TRUNCATE TABLE stg.expenses')
conn.commit()

rows = [
    (
        int(r.user_id),
        str(r.first_name),
        str(r.last_name),
        str(r.expense_month),
        float(r.housing),
        float(r.transport),
        float(r.food_groceries),
        float(r.utilities),
        float(r.insurance),
        float(r.entertainment),
        float(r.education),
        float(r.medical),
        float(r.clothing),
        float(r.savings_transfer),
        float(r.total_monthly_expenses),
        f'{DATA_DIR}\\monthly_expenses.csv',
        load_date,
        batch_id
    )
    for r in df.itertuples(index=False)
]

cursor.executemany("""
    INSERT INTO stg.expenses
    (user_id, first_name, last_name, expense_month,
     housing, transport, food_groceries, utilities, insurance,
     entertainment, education, medical, clothing,
     savings_transfer, total_monthly_expenses,
     source_file, load_date, batch_id)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", rows)

conn.commit()
cursor.close()

print(f'  stg.expenses: {len(rows)} rows loaded')

# ── 4. stg.savings ───────────────────────────────────────────────────────
print('Loading stg.savings ...')

df = pd.read_csv(f'{DATA_DIR}\\savings_investments.csv')
df.columns = df.columns.str.strip()

cursor = conn.cursor()
cursor.fast_executemany = True

cursor.execute('TRUNCATE TABLE stg.savings')
conn.commit()

rows = [
    (
        int(r.user_id),
        str(r.first_name),
        str(r.last_name),
        str(r.account_type),
        str(r.institution),
        float(r.current_balance),
        float(r.monthly_contribution),
        str(r.opened_date),
        f'{DATA_DIR}\\savings_investments.csv',
        load_date,
        batch_id
    )
    for r in df.itertuples(index=False)
]

cursor.executemany("""
    INSERT INTO stg.savings
    (user_id, first_name, last_name, account_type, institution,
     current_balance, monthly_contribution, opened_date,
     source_file, load_date, batch_id)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
""", rows)

conn.commit()
cursor.close()

print(f'  stg.savings: {len(rows)} rows loaded')

# ── 5. stg.events ────────────────────────────────────────────────────────
print('Loading stg.events ...')

df = pd.read_csv(f'{DATA_DIR}\\financial_events.csv')
df.columns = df.columns.str.strip()

df['interest_rate'] = pd.to_numeric(df['interest_rate'], errors='coerce')

cursor = conn.cursor()
cursor.fast_executemany = True

rows = [
    (
        str(r.event_id),
        int(r.user_id),
        str(r.first_name),
        str(r.last_name),
        str(r.event_type).upper().strip(),
        float(r.amount),
        None if pd.isna(r.interest_rate) else float(r.interest_rate),
        str(r.event_timestamp),
        f'{DATA_DIR}\\financial_events.csv',
        batch_id
    )
    for r in df.itertuples(index=False)
]

# Deduplicate
existing = set(r[0] for r in cursor.execute("SELECT event_id FROM stg.events").fetchall())
new_rows = [r for r in rows if r[0] not in existing]

if new_rows:
    cursor.executemany("""
        INSERT INTO stg.events
        (event_id, user_id, first_name, last_name,
         event_type, amount, interest_rate, event_timestamp,
         source_file, batch_id)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, new_rows)
    conn.commit()

cursor.close()

print(f'  stg.events: {len(new_rows)} rows loaded')

# ── VERIFICATION ─────────────────────────────────────────────────────────
print('\nVerification:')

cursor = conn.cursor()

for tbl in ['stg.salary', 'stg.debt', 'stg.expenses', 'stg.savings', 'stg.events']:
    count = cursor.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f'  {tbl:<20} {count:>4} rows')

cursor.close()
conn.close()

print('\nStaging load complete.')