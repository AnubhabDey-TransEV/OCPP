from peewee import Model, AutoField, CharField, IntegerField, FloatField, DateTimeField, MySQLDatabase

# Assuming you've already defined your database connection
db = MySQLDatabase('OCPP', user='ocpphandler', password='ocpp2024', host='localhost', port=3306)

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

# Create the tables
db.connect()
db.create_tables([Transaction, Reservation], safe=True)
