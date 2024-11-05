from peewee import (
    Model,
    CharField,
    MySQLDatabase,
    AutoField,
    DateTimeField,
    BooleanField,
    TextField,
)
import requests
import json
from datetime import datetime

# Database connection
DATABASE_URI = {
    "host": "localhost",
    "user": "ocpphandler",
    "password": "ocpp2024",
    "database": "OCPP",
}
db = MySQLDatabase(
    DATABASE_URI["database"],
    user=DATABASE_URI["user"],
    password=DATABASE_URI["password"],
    host=DATABASE_URI["host"],
    port=3306,
)


class BaseModel(Model):
    class Meta:
        database = db


class ChargerData(BaseModel):
    uid = CharField(unique=True)
    # Add other fields as necessary

    class Meta:
        table_name = "Charger_Data"


class UserAndCharger(BaseModel):
    id = AutoField()  # Auto-incrementing primary key
    uid = CharField()
    firstname = CharField()
    lastname = CharField()
    email = CharField()
    password = TextField()
    address = TextField()
    phonenumber = CharField()
    role = CharField()
    designation = TextField()
    created_at = DateTimeField()
    updated_at = DateTimeField()
    charger_uid = CharField()
    charger_name = CharField()
    charger_host = CharField()
    segment = CharField()
    subsegment = CharField()
    total_capacity = CharField()
    charger_type = CharField()
    parking = CharField()
    number_of_connectors = CharField()
    connector_type = CharField()
    connector_total_capacity = CharField()
    latitude = CharField()
    longitude = CharField()
    full_address = TextField()
    charger_use_type = CharField()
    twenty_four_seven_open_status = BooleanField()
    user_id = CharField()

    class Meta:
        table_name = "User_and_Charger"


# Ensure the table is created
db.connect()
db.create_tables([UserAndCharger])


# Function to make the POST request and save response
def fetch_and_store_data():
    url = "https://evchargercmsbackend.onrender.com/admin/getchargerbyuserid"
    headers = {
        "Content-Type": "application/json",
        "apiauthkey": "aBcD1eFgH2iJkLmNoPqRsTuVwXyZ012345678jasldjalsdjurewouroewiru",
    }

    for charger in ChargerData.select():
        uid = charger.uid
        response = requests.post(
            url, headers=headers, json={"get_charger_id": uid}
        )

        if response.status_code == 200:
            data = response.json()
            user_details = data["userdetails"]
            charger_details = data["user_chargerunit_details"]

            UserAndCharger.create(
                uid=user_details["uid"],
                firstname=user_details["firstname"],
                lastname=user_details["lastname"],
                email=user_details["email"],
                password=user_details["password"],
                address=user_details["address"],
                phonenumber=user_details["phonenumber"],
                role=user_details["role"],
                designation=user_details["designation"],
                created_at=datetime.fromisoformat(
                    user_details["createdAt"].replace("Z", "+00:00")
                ),
                updated_at=datetime.fromisoformat(
                    user_details["updatedAt"].replace("Z", "+00:00")
                ),
                charger_uid=charger_details["uid"],
                charger_name=charger_details["ChargerName"],
                charger_host=charger_details["Chargerhost"],
                segment=charger_details["Segment"],
                subsegment=charger_details["Subsegment"],
                total_capacity=charger_details["Total_Capacity"],
                charger_type=charger_details["Chargertype"],
                parking=charger_details["parking"],
                number_of_connectors=charger_details["number_of_connectors"],
                connector_type=charger_details["Connector_type"],
                connector_total_capacity=charger_details[
                    "connector_total_capacity"
                ],
                latitude=charger_details["lattitude"],
                longitude=charger_details["longitute"],
                full_address=charger_details["full_address"],
                charger_use_type=charger_details["charger_use_type"],
                twenty_four_seven_open_status=charger_details[
                    "twenty_four_seven_open_status"
                ]
                == "true",
                user_id=charger_details["userId"],
            )
        else:
            print(
                f"Failed to fetch data for UID {uid}: {response.status_code}"
            )


if __name__ == "__main__":
    fetch_and_store_data()
