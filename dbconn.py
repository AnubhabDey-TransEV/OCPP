from decouple import config
from playhouse.pool import PooledMySQLDatabase  # Use PooledMySQLDatabase for connection pooling
import ssl

# Load database configuration from environment variables
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT', cast=int)

# Construct the pooled database connection
db = PooledMySQLDatabase(
    DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    max_connections=100,  # Set up to 52 concurrent connections
    stale_timeout=300,  # Recycle connections after 5 minutes of inactivity
    timeout=30  # Optional: Timeout for connection attempts
)

def get_database():
    return db
