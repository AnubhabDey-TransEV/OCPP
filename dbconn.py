import asyncio
from peewee import Model
from playhouse.pool import PooledMySQLDatabase
from decouple import config

# Load DB config from .env
DB_NAME = config("DB_NAME")
DB_USER = config("DB_USER")
DB_PASSWORD = config("DB_PASSWORD")
DB_HOST = config("DB_HOST")
DB_PORT = config("DB_PORT", cast=int)

# Create pooled DB instance
db = PooledMySQLDatabase(
    DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    max_connections=100,
    stale_timeout=300,
    timeout=30,
)

def get_database():
    return db

# 🔧 Connection safety logic
def ensure_connection():
    try:
        db.connection().ping(reconnect=True)
    except Exception:
        db.close()
        db.connect()

# 🧙 Universal patch for .create() to always check connection
_original_create = Model.create

def patched_create(cls, **kwargs):
    ensure_connection()
    return _original_create.__func__(cls, **kwargs)

Model.create = classmethod(patched_create)

# 🔁 Periodic heartbeat task to keep DB pool warm
async def keep_db_alive():
    while True:
        try:
            db.connection().ping(reconnect=True)
            print("✅ DB heartbeat successful.")
        except Exception as e:
            print(f"⚠️ DB heartbeat failed: {e}")
            db.close()
            db.connect()
        await asyncio.sleep(3600)  # Ping every hour
