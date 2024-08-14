from peewee import Model, AutoField, CharField, DateTimeField, IntegerField, FloatField
from dbconn import get_database

# Get the database connection
db = get_database()

class BaseModel(Model):
    class Meta:
        database = db

class Transaction(BaseModel):
    id = AutoField()
    charger_id = CharField()
    connector_id = IntegerField()
    meter_start = FloatField()
    meter_stop = FloatField()
    total_consumption = FloatField()  # You can manually compute this in your code
    start_time = DateTimeField()
    stop_time = DateTimeField()

    class Meta:
        table_name = 'transactions'
        indexes = (
            (('charger_id', 'connector_id'), False),
        )

class Reservation(BaseModel):
    id = AutoField()
    charger_id = CharField()
    connector_id = IntegerField()
    id_tag = CharField()
    expiry_date = DateTimeField()
    reservation_id = IntegerField(unique=True)
    reserved_at = DateTimeField()
    from_time = DateTimeField()  # The start time of the reservation
    to_time = DateTimeField()  # The end time of the reservation
    status = CharField()  # Status can be 'Reserved' or 'Cancelled'

    class Meta:
        table_name = 'reservations'

class OCPPMessageCMS(BaseModel):
    id = AutoField()
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField()

    class Meta:
        table_name = 'CMS_to_Charger'

class OCPPMessageCharger(BaseModel):
    id = AutoField()
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField()

    class Meta:
        table_name = 'Charger_to_CMS'

# Create the tables
db.connect()
db.create_tables([Transaction, Reservation, OCPPMessageCMS, OCPPMessageCharger], safe=True)
