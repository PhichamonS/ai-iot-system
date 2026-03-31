import os
import psycopg2
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cursor = conn.cursor()

# 1. Enable TimescaleDB extension
cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
conn.commit()

# 2. Create table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS energy_data (
        time        TIMESTAMPTZ       NOT NULL,
        meter_id    VARCHAR(255)    NOT NULL,
        consumption_kw DOUBLE PRECISION,
        PRIMARY KEY (time, meter_id)
    );
""")
conn.commit()

# 3. Convert to hypertable (TimescaleDB magic)
cursor.execute("""
    SELECT create_hypertable('energy_data', 'time', if_not_exists => TRUE);
""")
conn.commit()

cursor.close()
conn.close()
print("Hypertable created successfully!")