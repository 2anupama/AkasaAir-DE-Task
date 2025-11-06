"""
Akasa Air Data Engineer - In-memory Pipeline
✅ Clean ✅ Secure ✅ TZ-aware ✅ Handles missing values ✅ Scalable ✅ Simple
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd

# ================== CONFIG ==================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")


CUSTOMERS_CSV = os.path.join(DATA_DIR, "task_DE_new_customers.csv")
ORDERS_XML = os.path.join(DATA_DIR, "task_DE_new_orders.xml")

SOURCE_TZ = "Asia/Kolkata"
BUSINESS_TZ = "Asia/Kolkata"
NOW_UTC = datetime.now(timezone.utc)

# ================== LOGGING (no PII) ==================
logger = logging.getLogger("akasa_inmemory")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

def mask_mobile(m: str) -> str:
    m = str(m)
    return m[:2] + "*"*(len(m)-4) + m[-2:] if len(m) >= 4 else "*"*len(m)

# ================== LOAD CUSTOMERS ==================
def load_customers_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype="string",
                     na_values=["", "NA", "null", "None", "NaN"])
    df["mobile_number"] = df["mobile_number"].str.replace(r"\D+", "", regex=True)
    df["region"] = df["region"].fillna("Unknown")
    df = df.dropna(subset=["customer_name", "mobile_number"])
    df = df.drop_duplicates(subset=["mobile_number"]).reset_index(drop=True)
    logger.info("Customers loaded: %d", len(df))
    return df

# ================== LOAD ORDERS ==================
def load_orders_xml(path: str) -> pd.DataFrame:
    df = pd.read_xml(path, dtype="string")
    df["mobile_number"] = df["mobile_number"].str.replace(r"\D+", "", regex=True)
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)

    src = ZoneInfo(SOURCE_TZ)
    df["order_dt_local"] = pd.to_datetime(df["order_date_time"], errors="coerce")
    df["order_dt_local"] = df["order_dt_local"].dt.tz_localize(
        src, ambiguous="NaT", nonexistent="shift_forward")
    df["order_dt_utc"] = df["order_dt_local"].dt.tz_convert(timezone.utc)

    df = df.dropna(subset=["order_id", "mobile_number", "order_dt_utc"])

    # Unique orders only (avoid counting line items)
    df = (df.groupby("order_id", as_index=False)
            .agg({"mobile_number": "first",
                  "order_dt_utc": "first",
                  "total_amount": "first"}))

    logger.info("Orders loaded (unique): %d", len(df))
    return df

# ================== KPIs ==================
def kpi_repeat_customers(orders):
    o = orders.dropna(subset=["mobile_number"])
    return (o.groupby("mobile_number")
             .size().reset_index(name="order_count")
             .query("order_count > 1")
             .sort_values("order_count", ascending=False))

def kpi_monthly_trends(orders):
    biz = ZoneInfo(BUSINESS_TZ)
    month = orders["order_dt_utc"].dt.tz_convert(biz).dt.to_period("M").astype(str)
    return (orders.assign(year_month=month)
             .groupby("year_month").size()
             .reset_index(name="total_orders")
             .sort_values("year_month"))

def kpi_regional_revenue(customers, orders):
    merged = orders.merge(customers, on="mobile_number", how="left")
    merged["region"] = merged["region"].fillna("Unknown")
    return (merged.groupby("region")["total_amount"]
             .sum()
             .reset_index(name="revenue")
             .sort_values("revenue", ascending=False))

def kpi_top_spenders_30d(customers, orders_all):
    biz = ZoneInfo(BUSINESS_TZ)
    cutoff_biz = NOW_UTC.astimezone(biz) - timedelta(days=30)
    cutoff_utc = cutoff_biz.astimezone(timezone.utc)

    recent = orders_all.loc[orders_all["order_dt_utc"] >= cutoff_utc]
    merged = recent.merge(customers, on="mobile_number", how="left")
    merged["customer_name"] = merged["customer_name"].fillna("Unknown")

    return (merged.groupby("customer_name")["total_amount"]
             .sum()
             .reset_index(name="total_spent")
             .sort_values("total_spent", ascending=False))

# ================== OUTPUT HELPERS ==================
def print_df(title, df):
    print(f"\n=== {title} ===")
    print(df.to_string(index=False) if not df.empty else "(no rows)")

# ================== MAIN ==================
def main():
    try:
        customers = load_customers_csv(CUSTOMERS_CSV)
        orders = load_orders_xml(ORDERS_XML)

        repeat = kpi_repeat_customers(orders)
        monthly = kpi_monthly_trends(orders)
        regional = kpi_regional_revenue(customers, orders)
        top30 = kpi_top_spenders_30d(customers, orders)

        print_df("Repeat Customers", repeat)
        print_df("Monthly Order Trends", monthly)
        print_df("Regional Revenue", regional)
        print_df("Top Spenders (Last 30 Days)", top30)

        if not repeat.empty:
            masked = repeat["mobile_number"].head(3).map(mask_mobile).tolist()
            logger.info("Masked sample customers: %s", masked)

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
