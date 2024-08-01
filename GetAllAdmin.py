import requests
import itertools
from peewee import MySQLDatabase, Model, TextField, IntegerField, FloatField, BooleanField, DateTimeField, DateField
from playhouse.reflection import Introspector
from datetime import datetime, date

# Step 1: Fetch the JSON data from the API endpoint
response = requests.get("https://evchargercmsbackend.onrender.com/admin/getalladmindata")
data = response.json()

# Step 2: Combine all keys from the JSON data
all_keys = set(itertools.chain.from_iterable(d.keys() for d in data))

# Step 3: Determine the field types dynamically
field_types = {}
for key in all_keys:
    for entry in data:
        if key in entry:
            value = entry[key]
            if isinstance(value, int):
                field_types[key] = IntegerField
            elif isinstance(value, float):
                field_types[key] = FloatField
            elif isinstance(value, bool):
                field_types[key] = BooleanField
            elif isinstance(value, datetime):
                field_types[key] = DateTimeField
            elif isinstance(value, date):
                field_types[key] = DateField
            else:
                field_types[key] = TextField
            break

# Step 4: Connect to the MySQL database
db = MySQLDatabase('OCPP', user='ocpphandler', password='ocpp2024', host='localhost', port=3306)

# Step 5: Dynamically create the Peewee model with table name Admin_Data
class BaseModel(Model):
    class Meta:
        database = db
        table_name = 'Admin_Data'

# Get existing columns in the database table
introspector = Introspector.from_database(db)
existing_columns = introspector.introspect('Admin_Data').columns

# Add fields dynamically, checking if they already exist
for key, field_type in field_types.items():
    if key not in existing_columns:
        field = field_type(null=True)
        field.add_to_class(BaseModel, key)

# Bind the model to the database
BaseModel._meta.database = db

# Create the table in the database if it doesn't exist
if not BaseModel.table_exists():
    BaseModel.create_table()

# Step 6: Function to convert values to correct types
def convert_value(value):
    if isinstance(value, list) or isinstance(value, dict):
        return str(value)
    return value

# Step 7: Insert parsed data into the database
with db.atomic():
    for entry in data:
        entry_converted = {key: convert_value(value) for key, value in entry.items()}
        BaseModel.create(**entry_converted)
