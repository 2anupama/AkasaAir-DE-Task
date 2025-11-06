import os
import sys
from urllib.parse import quote_plus
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

# ----------------- Config -----------------
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

def build_db_url() -> str:
    url = os.getenv("DB_URL")
    if url:
        return url
    user = os.getenv("DB_USER")
    pwd  = quote_plus(os.getenv("DB_PASSWORD") or "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME", "akasa")
    if not user or not name:
        raise RuntimeError("DB_URL missing and DB_USER/DB_NAME not set in .env")
    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"

DB_URL = build_db_url()
BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Kolkata")
DATA_HORIZON = (os.getenv("DATA_HORIZON", "24 MONTH") or "").strip()  # "" disables horizon

def get_engine() -> Engine:
    return create_engine(DB_URL, pool_pre_ping=True, future=True)

# ----------------- Time helpers -----------------
def cutoff_30d_utc_naive():
    biz = ZoneInfo(BUSINESS_TZ)
    cutoff = datetime.now(timezone.utc).astimezone(biz) - timedelta(days=30)
    return cutoff.astimezone(timezone.utc).replace(tzinfo=None)

# ----------------- Summary DDL/refresh -----------------
SQL_CREATE_SUMMARY = text("""
CREATE TABLE IF NOT EXISTS orders_monthly (
  yr SMALLINT NOT NULL,
  mn TINYINT NOT NULL,
  total_orders BIGINT NOT NULL,
  PRIMARY KEY (yr, mn)
) ENGINE=InnoDB;
""")

SQL_INSERT_SUMMARY_NO_HORIZON = text("""
INSERT INTO orders_monthly (yr, mn, total_orders)
SELECT
  YEAR(order_date_time_utc)  AS yr,
  MONTH(order_date_time_utc) AS mn,
  COUNT(*) AS total_orders
FROM orders
GROUP BY YEAR(order_date_time_utc), MONTH(order_date_time_utc);
""")

def sql_insert_summary_with_horizon(horizon_literal: str):
    # horizon_literal is static from env (e.g., "24 MONTH"); no user input
    return text(f"""
INSERT INTO orders_monthly (yr, mn, total_orders)
SELECT
  YEAR(order_date_time_utc)  AS yr,
  MONTH(order_date_time_utc) AS mn,
  COUNT(*) AS total_orders
FROM orders
WHERE order_date_time_utc >= (NOW() - INTERVAL {horizon_literal})
GROUP BY YEAR(order_date_time_utc), MONTH(order_date_time_utc);
""")

def refresh_monthly_summary(conn):
    """Rebuild monthly summary using TWO executes (TRUNCATE then INSERT)."""
    conn.execute(SQL_CREATE_SUMMARY)
    conn.execute(text("TRUNCATE TABLE orders_monthly"))
    if DATA_HORIZON:
        conn.execute(sql_insert_summary_with_horizon(DATA_HORIZON))
    else:
        conn.execute(SQL_INSERT_SUMMARY_NO_HORIZON)

# ----------------- KPI SQL (pure SQL) -----------------
SQL_REPEAT = text("""
SELECT mobile_number, COUNT(*) AS order_count
FROM orders
GROUP BY mobile_number
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, mobile_number ASC;
""")

SQL_MONTHLY = text("""
SELECT yr, mn, total_orders
FROM orders_monthly
ORDER BY yr, mn;
""")

def sql_regional():
    if DATA_HORIZON:
        return text(f"""
            SELECT c.region, SUM(o.total_amount) AS revenue
            FROM orders o
            JOIN customers c ON c.mobile_number = o.mobile_number
            WHERE o.order_date_time_utc >= (NOW() - INTERVAL {DATA_HORIZON})
            GROUP BY c.region
            ORDER BY SUM(o.total_amount) DESC;
        """)
    else:
        return text("""
            SELECT c.region, SUM(o.total_amount) AS revenue
            FROM orders o
            JOIN customers c ON c.mobile_number = o.mobile_number
            GROUP BY c.region
            ORDER BY SUM(o.total_amount) DESC;
        """)

SQL_TOP_30D = text("""
SELECT c.customer_name, SUM(o.total_amount) AS total_spent
FROM orders o
JOIN customers c ON c.mobile_number = o.mobile_number
WHERE o.order_date_time_utc >= :cutoff
GROUP BY c.customer_name
ORDER BY SUM(o.total_amount) DESC;
""")

# ----------------- Print helper -----------------
def df_print(title, df):
    print(f"\n=== {title} ===")
    if df.empty:
        print("No data found")
    else:
        print(df.to_string(index=False))

# ----------------- Main -----------------
def main():
    try:
        engine = get_engine()

        # 1) Transactional summary refresh (separate TRUNCATE and INSERT)
        with engine.begin() as tx:
            refresh_monthly_summary(tx)

        # 2) KPIs (streaming)
        with engine.connect().execution_options(stream_results=True) as conn:
            repeat  = pd.read_sql(SQL_REPEAT, conn)
            monthly = pd.read_sql(SQL_MONTHLY, conn)
            regional = pd.read_sql(sql_regional(), conn)
            top30   = pd.read_sql(SQL_TOP_30D, conn, params={"cutoff": cutoff_30d_utc_naive()})

        # 3) Print only
        df_print("Repeat Customers", repeat)
        df_print("Monthly Order Trends (yr, mn, total_orders)", monthly)
        df_print("Regional Revenue", regional)
        df_print("Top Customers by Spend (Last 30 Days)", top30)

    except SQLAlchemyError as e:
        if isinstance(e, DBAPIError) and e.orig:
            print(f"Database error: {type(e.orig).__name__}: {e.orig}", file=sys.stderr)
        else:
            print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
