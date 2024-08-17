from decouple import config
from peewee import MySQLDatabase
import ssl

# Load database configuration from environment variables
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT', cast=int)

# Construct the database connection
db = MySQLDatabase(
    DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    # ssl=True
)

def get_database():
    return db
