import logging
from datetime import datetime, timedelta, timezone
from peewee import IntegrityError, fn
from models import Logs, db

class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        # Avoid recursive logging by not logging the database operations themselves
        if record.name in ('peewee', 'database_logger'):
            return

        # Convert the log entry to string and check if it contains the word "heartbeat"
        log_entry = self.format(record)
        # if "heartbeat" in log_entry.lower():  # Case-insensitive check
        #     return  # Skip logging if "heartbeat" is found

        try:
            # Convert the timestamp to IST (UTC+5:30)
            utc_timestamp = datetime.fromtimestamp(record.created, timezone.utc)
            ist_offset = timedelta(hours=5, minutes=30)
            ist_timestamp = utc_timestamp + ist_offset

            # Start a transaction to ensure atomicity
            with db.atomic():
                # Prepare the log entry details
                log_level = record.levelname
                file_origin = record.pathname.split("/")[-1] if "/" in record.pathname else record.pathname.split("\\")[-1]
                error_details = record.exc_text if record.exc_info else None

                # Insert the new log into the database with IST timestamp
                Logs.create(
                    log_message=log_entry,
                    log_level=log_level,
                    timestamp=ist_timestamp,  # Store in IST
                    file_origin=file_origin,
                    error_details=error_details
                )

                # Calculate the date one year ago in IST
                one_year_ago = ist_timestamp - timedelta(days=365)

                # Delete logs that are older than one year from the IST timestamp
                Logs.delete().where(Logs.timestamp < one_year_ago.date()).execute()

        except IntegrityError as e:
            # Print errors to avoid further logging
            print(f"Failed to log to database: {e}")
        except Exception as e:
            print(f"Unexpected error during logging: {e}")

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the database log handler
    db_handler = DatabaseLogHandler()
    db_handler.setLevel(logging.DEBUG)

    # Set a formatter to ensure consistent log formatting
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    db_handler.setFormatter(formatter)

    # Add the database handler to the root logger
    logger.addHandler(db_handler)

    return logger
