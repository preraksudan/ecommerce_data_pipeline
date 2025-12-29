# import psycopg2
# import logging
# import sys
# import pandas as pd

# # ==========================================================
# # LOGGING CONFIG
# # ==========================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)],
#     force=True
# )

# logger = logging.getLogger(__name__)

# # ==========================================================
# # DB CONFIG
# # ==========================================================

# DB_CONFIG = {
#     "host": "localhost",
#     "dbname": "mydatabase",
#     "user": "myuser",
#     "password": "mypassword",
#     "port": 5432
# }

# # ==========================================================
# # CREATE TABLE SQL
# # ==========================================================
# CREATE_TABLE_SQL = """
# CREATE TABLE IF NOT EXISTS orders (
#     order_id TEXT PRIMARY KEY,
#     customer_id TEXT NOT NULL,
#     order_status TEXT NOT NULL,
#     order_purchase_timestamp TIMESTAMP NOT NULL,
#     order_purchase_date DATE,
#     order_purchase_year INT,
#     order_purchase_month INT,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
# """

# # ==========================================================
# # INSERT SQL
# # ==========================================================
# INSERT_SQL = """
# INSERT INTO orders (
#     order_id,
#     customer_id,
#     order_status,
#     order_purchase_timestamp,
#     order_purchase_date,
#     order_purchase_year,
#     order_purchase_month
# )
# VALUES (%s, %s, %s, %s, %s, %s, %s)
# ON CONFLICT (order_id) DO NOTHING;
# """

# # ==========================================================
# # LOAD FUNCTION
# # ==========================================================
# def load_orders(df: pd.DataFrame):

#     logger.info("========== LOAD STAGE STARTED ==========")

#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()

#     try:
#         # 1 Table Existance
#         logger.info("Ensuring target table exists")
#         cur.execute(CREATE_TABLE_SQL)
#         conn.commit()

#         # 2 Insert records
#         rows = 0
#         for _, row in df.iterrows():
#             cur.execute(INSERT_SQL, (
#                 row["order_id"],
#                 row["customer_id"],
#                 row["order_status"],
#                 row["order_purchase_timestamp"],
#                 row["order_purchase_date"],
#                 row["order_purchase_year"],
#                 row["order_purchase_month"]
#             ))
#             rows += 1

#         conn.commit()
#         logger.info(f"Rows attempted to load: {rows}")

#     except Exception as e:
#         conn.rollback()
#         logger.exception("Load failed — transaction rolled back")
#         raise

#     finally:
#         cur.close()
#         conn.close()

#     logger.info("========== LOAD STAGE COMPLETED ==========")



"""
    Batch execute query for uploading data...
"""
import os
import psycopg2
import logging
import sys
import pandas as pd
from dotenv import load_dotenv
import importlib.util



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE_PATH = os.path.join(BASE_DIR, "..", "config", "db_config.py")

spec = importlib.util.spec_from_file_location("db_config", DB_FILE_PATH)

db_config_module = importlib.util.module_from_spec(spec)

spec.loader.exec_module(db_config_module)

DB_CONFIG = db_config_module.DB_CONFIG


from psycopg2.extras import execute_batch
load_dotenv()

# ==========================================================
# LOGGING CONFIG
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)

logger = logging.getLogger(__name__)

# ==========================================================
# DB CONFIG
# ==========================================================

print(os.getenv('DB_HOST'))
print(os.getenv('DB_NAME'))
print(os.getenv('DB_USER'))
print(os.getenv('DB_PASS'))
print(os.getenv('DB_PORT'))

# ==========================================================
# SQL
# ==========================================================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_purchase_date DATE,
    order_purchase_year INT,
    order_purchase_month INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_SQL = """
INSERT INTO orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_purchase_date,
    order_purchase_year,
    order_purchase_month
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO NOTHING;
"""

# ==========================================================
# LOAD FUNCTION
# ==========================================================
def load_orders(df: pd.DataFrame):

    logger.info("========== LOAD STAGE STARTED ==========")

    records = [
        (
            row["order_id"],
            row["customer_id"],
            row["order_status"],
            row["order_purchase_timestamp"],
            row["order_purchase_date"],
            row["order_purchase_year"],
            row["order_purchase_month"]
        )
        for _, row in df.iterrows()
    ]

    logger.info(f"Prepared {len(records)} records for batch insert")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # Ensure table exists
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        # Batch insert
        execute_batch(
            cur,
            INSERT_SQL,
            records,
            page_size=1000
        )

        conn.commit()
        logger.info("Batch insert completed successfully")

    except Exception:
        conn.rollback()
        logger.exception("Load failed — transaction rolled back")
        raise

    finally:
        cur.close()
        conn.close()

    logger.info("========== LOAD STAGE COMPLETED ==========")
