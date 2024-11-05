import requests
import json
from peewee import (
    MySQLDatabase,
    Model,
    TextField,
    FloatField,
    BooleanField,
    IntegerField,
    SQL,
)

# Fetch JSON data from the API endpoint
response = requests.get(
    "https://evchargercmsbackend.onrender.com/admin/listofcharges"
)

# Connect to the MySQL database
database = MySQLDatabase(
    "OCPP",
    user="ocpphandler",
    password="ocpp2024",
    host="localhost",
    port=3306,
)


class BaseModel(Model):
    class Meta:
        database = database
        table_name = "Charger_Data"


# Create a function to determine field types dynamically
def create_dynamic_model(columns):
    fields = {"id": IntegerField(primary_key=True)}

    for column_name, value in columns.items():
        if isinstance(value, int):
            fields[column_name] = IntegerField(null=True)
        elif isinstance(value, float):
            fields[column_name] = FloatField(null=True)
        elif isinstance(value, bool):
            fields[column_name] = BooleanField(null=True)
        else:
            fields[column_name] = TextField(null=True)

    dynamic_model = type("Charger_Data", (BaseModel,), fields)
    return dynamic_model


# Add missing columns to the existing table
def add_missing_columns(model, columns):
    existing_columns = {field.name for field in model._meta.sorted_fields}

    with database.atomic():
        for column_name, value in columns.items():
            if column_name not in existing_columns:
                if isinstance(value, int):
                    field = IntegerField(null=True)
                elif isinstance(value, float):
                    field = FloatField(null=True)
                elif isinstance(value, bool):
                    field = BooleanField(null=True)
                else:
                    field = TextField(null=True)

                # Add the new column to the model
                field.add_to_class(model, column_name)

                # Add the new column to the database
                database.execute_sql(
                    f"ALTER TABLE {model._meta.table_name} ADD COLUMN {column_name} {field.ddl()}"
                )


# Load JSON data
json_data = json.loads(response.content)

# Extract columns from the first JSON object
columns = json_data[0]

# Create a dynamic model for the Admin_Data table
DynamicModel = create_dynamic_model(columns)

# Connect to the database
database.connect()

# Create the table if it doesn't exist
if not DynamicModel.table_exists():
    DynamicModel.create_table()
else:
    # Add any missing columns to the existing table
    add_missing_columns(DynamicModel, columns)

# Insert data
with database.atomic():
    for item in json_data:
        DynamicModel.create(**item)

# Close the database connection
database.close()
