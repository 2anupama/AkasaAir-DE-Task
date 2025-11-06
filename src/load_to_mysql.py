import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

# ---------- Settings ----------
DATA_DIR = os.getenv("DATA_DIR", "./data")
CUSTOMERS_CSV = os.path.join(DATA_DIR, "task_DE_new_customers.csv")
ORDERS_XML = os.path.join(DATA_DIR, "task_DE_new_orders.xml")

SOURCE_TZ = os.getenv("SOURCE_TZ", "Asia/Kolkata")

# ---------- Build DB URL ----------
def get_db_url():
    url = os.getenv("DB_URL")
    if url:
        return url
    u = os.getenv("DB_USER")
    p = quote_plus(os.getenv("DB_PASSWORD") or "")
    h = os.getenv("DB_HOST", "127.0.0.1")
    pt = os.getenv("DB_PORT", "3306")
    n = os.getenv("DB_NAME", "akasa")
    if not u or not n:
        raise RuntimeError("DB credentials missing in .env")
    return f"mysql+pymysql://{u}:{p}@{h}:{pt}/{n}?charset=utf8mb4"

DB_URL = get_db_url()

# ---------- Loaders ----------
def load_customers():
    df = pd.read_csv(CUSTOMERS_CSV, dtype="string")
    df = df.drop_duplicates(subset=["mobile_number"]).reset_index(drop=True)
    return df

def load_orders():
    df = pd.read_xml(ORDERS_XML)

    df["order_date_time"] = pd.to_datetime(df["order_date_time"], errors="coerce")

    tz = ZoneInfo(SOURCE_TZ)
    df["order_dt_utc"] = (
        df["order_date_time"]
        .dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
        .dt.tz_convert(timezone.utc)
    )

    df = df.dropna(subset=["order_dt_utc"]).reset_index(drop=True)

    # keep 1 row per order_id
    df_unique = df.groupby("order_id", as_index=False).agg({
        "mobile_number": "first",
        "order_dt_utc": "first",
        "total_amount": "first"
    })

    return df_unique

# ---------- DB Upload ----------
def upload_to_mysql():
    try:
        engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

        with engine.begin() as conn:
            customers = load_customers()
            orders = load_orders()

            customers.to_sql("customers", conn, if_exists="replace", index=False)
            orders.to_sql("orders", conn, if_exists="replace", index=False)

        print("Successfully loaded customers & orders into MySQL!")

    except SQLAlchemyError as e:
        print(f"DB Error: {e}", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        print(f"Unexpected Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    upload_to_mysql()
