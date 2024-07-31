import requests
import json
from peewee import MySQLDatabase, Model, CharField, FloatField, BooleanField, IntegerField, TextField

response = requests.get(
    url='https://evchargercmsbackend.onrender.com/admin/listofcharges'
)

database = MySQLDatabase(
    'OCPP',
    user='ocpphandler',
    password='ocpp2024',
    host='localhost',
    port=3306
)

class BaseModel(Model):
    class Meta:
        database = database

def create_dynamic_model(table_name, columns):
    fields = {
        'id': IntegerField(primary_key=True)
    }

    for column_name, value in columns.items():
        if isinstance(value, int):
            fields[column_name] = IntegerField(null=True)
        elif isinstance(value, float):
            fields[column_name] = FloatField(null=True)
        elif isinstance(value, bool):
            fields[column_name] = BooleanField(null=True)
        else:
            fields[column_name] = TextField(null=True)
    
    dynamic_model = type(table_name, (BaseModel,), fields)
    return dynamic_model

json_data = json.loads(response.content)

# Extract columns from the first JSON object
columns = json_data[0]

# Create a dynamic model
DynamicModel = create_dynamic_model('DynamicCharger', columns)

# Connect to the database
database.connect()

# Create the table if it doesn't exist
if not DynamicModel.table_exists():
    DynamicModel.create_table()

# Insert data
for item in json_data:
    DynamicModel.create(**item)

# Close the database connection
database.close()