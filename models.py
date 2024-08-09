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

# Create the table (you can run this part once to create the table)
db.connect()
db.create_tables([Transaction], safe=True)
