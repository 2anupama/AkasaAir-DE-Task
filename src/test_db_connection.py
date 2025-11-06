import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

# Load .env from current or parent dirs
load_dotenv(find_dotenv(usecwd=True))

# Build DB URL safely (encodes special chars like @ : # &)
user = os.getenv("DB_USER")
password = quote_plus(os.getenv("DB_PASSWORD") or "")
host = os.getenv("DB_HOST", "127.0.0.1")
port = os.getenv("DB_PORT", "3306")
db_name = os.getenv("DB_NAME", "akasa")

db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}?charset=utf8mb4"

print("\nTrying connection with:")
print(f"User: {user}")
print(f"Host: {host}")
print(f"Database: {db_name}")

try:
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar_one()
        db = conn.execute(text("SELECT DATABASE()")).scalar_one()
        print("\n✅ MySQL Connection Successful!")
        print(f"SELECT 1 => {result}")
        print(f"Connected Database => {db}\n")

except Exception as e:
    print("\n❌ MySQL Connection Failed!")
    print("Error =>", e, "\n")
