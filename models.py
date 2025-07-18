import uuid
from datetime import datetime

from peewee import (
    CharField,
    DateTimeField,
    DecimalField,
    FloatField,
    ForeignKeyField,
    IntegerField,
    Model,
)

from dbconn import get_database

# Get the database connection
db = get_database()


def getUUID():
    return str(uuid.uuid4())


class BaseModel(Model):
    class Meta:
        database = db


# class NetworkAnalytics(Model):
#     event_type = (
#         CharField()
#     )  # 'connect', 'disconnect', 'request', or 'response'
#     ev_id = CharField(
#         null=True
#     )  # EV identifier for WebSocket connections, null for API events
#     ip_address = CharField()  # IP address associated with the event
#     ip_information = TextField()
#     endpoint = (
#         CharField()
#     )  # Charger ID for WebSocket, API client endpoint for requests, 'CMS' for responses
#     request_data = TextField(
#         null=True
#     )  # JSON or string format of request data
#     response_data = TextField(
#         null=True
#     )  # JSON or string format of response data
#     timestamp = DateTimeField(
#         default=datetime.now
#     )  # Date and time of the event

#     class Meta:
#         database = db
#         table_name = "network_analytics"


# class Logs(Model):
#     id = AutoField()
#     uuid = UUIDField(default=getUUID, unique=True)
#     log_message = TextField()
#     log_level = CharField()  # e.g., INFO, ERROR, DEBUG
#     timestamp = DateTimeField()
#     file_origin = CharField()  # File which originated the log
#     error_details = TextField(null=True)  # Store error details if any

#     class Meta:
#         database = db
#         table_name = "logs"


class Transaction(BaseModel):
    uuiddb = CharField(default=getUUID)
    charger_id = CharField()
    connector_id = IntegerField()
    meter_start = FloatField()
    meter_stop = FloatField(null=True)
    total_consumption = FloatField(null=True)
    start_time = DateTimeField()
    stop_time = DateTimeField(null=True)
    id_tag = CharField()
    transaction_id = IntegerField(unique=True, null=True)
    
    class Meta:
        table_name = "transactions"
        indexes = ((("charger_id", "connector_id", "id_tag"), False),)



class Reservation(BaseModel):
    uuiddb = CharField(default=getUUID)
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
        table_name = "reservations"


class OCPPMessageCMS(BaseModel):
    uuiddb = CharField(default=getUUID)
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField()

    class Meta:
        table_name = "CMS_to_Charger"


class OCPPMessageCharger(BaseModel):
    uuiddb = CharField(default=getUUID)
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField()

    class Meta:
        table_name = "Charger_to_CMS"


class QRCodeData(BaseModel):
    uuiddb = CharField(default=getUUID)
    charger_id = CharField()
    charger_serial_number = CharField()
    image_path = CharField()  # Add this field
    filename = CharField()  # Add this field
    generation_date = DateTimeField()

    class Meta:
        table_name = "qr_code_data"


class Analytics(BaseModel):
    uuiddb = CharField(default=getUUID)
    charger_id = CharField()
    timestamp = DateTimeField()
    total_uptime = CharField()  # Storing in a human-readable format
    uptime_percentage = FloatField()
    total_transactions = IntegerField()
    total_electricity_used_kwh = FloatField()
    occupancy_rate_percentage = FloatField()
    average_session_duration = CharField()  # Storing in a human-readable format
    peak_usage_times = CharField()  # Storing as a comma-separated string

    class Meta:
        table_name = "analytics"


class Wallet(BaseModel):
    uid = CharField(unique=True)  # Wallet UID (for internal tracking)
    user_id = CharField()  # Directly store the userId from the external API
    balance = DecimalField(default=0.0)  # Wallet balance

    class Meta:
        table_name = "wallets"


class WalletRecharge(BaseModel):
    user_id = CharField()
    wallet_id = ForeignKeyField(Wallet, backref="recharges")
    balance_before = FloatField()
    recharge_amount = FloatField()
    balance_after = FloatField()
    recharged_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "wallet_recharge"


class WalletTransactions(BaseModel):
    user_id = CharField()  # The ID of the user involved in the transaction
    wallet_id = ForeignKeyField(
        Wallet, backref="transactions"
    )  # Foreign key to the wallet
    transaction_type = CharField()  # Either 'credit' or 'debit'
    transaction_time = DateTimeField(
        default=datetime.now
    )  # Timestamp of the transaction
    amount = FloatField()  # Amount involved in the transaction
    balance_before = FloatField()  # Balance before the transaction
    balance_after = FloatField()  # Balance after the transaction

    class Meta:
        table_name = "wallet_transactions"  # Define the table name


# Create the tables
db.connect()
db.create_tables(
    [
        Transaction,
        Reservation,
        OCPPMessageCMS,
        OCPPMessageCharger,
        QRCodeData,
        Analytics,
        # Logs,
        Wallet,
        WalletRecharge,
        WalletTransactions,
        # NetworkAnalytics,
    ],
    safe=True,
)
