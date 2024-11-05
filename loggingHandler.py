import logging
from datetime import datetime, timedelta, timezone
from peewee import IntegrityError, OperationalError
from models import Logs, db
import time  # To add a delay between retries


class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        # Avoid recursive logging by not logging the database operations themselves
        if record.name in ("peewee", "database_logger"):
            return

        log_entry = self.format(record)
        error_details = None  # Initialize error_details

        # Check if the log message contains the word "error"
        if "error" in log_entry.lower():
            # Extract everything after "error" (case-insensitive)
            error_details = log_entry.lower().split("error", 1)[-1].strip()

        retries = 5  # Number of retry attempts
        delay = 1  # Initial delay between retries in seconds

        for attempt in range(retries):
            try:
                # Convert the timestamp to IST (UTC+5:30)
                utc_timestamp = datetime.fromtimestamp(
                    record.created, timezone.utc
                )
                ist_offset = timedelta(hours=5, minutes=30)
                ist_timestamp = utc_timestamp + ist_offset

                # Start a transaction to ensure atomicity
                with db.atomic():
                    # Prepare the log entry details
                    log_level = record.levelname
                    file_origin = (
                        record.pathname.split("/")[-1]
                        if "/" in record.pathname
                        else record.pathname.split("\\")[-1]
                    )

                    # Insert the new log into the database with IST timestamp
                    Logs.create(
                        log_message=log_entry,
                        log_level=log_level,
                        timestamp=ist_timestamp,  # Store in IST
                        file_origin=file_origin,
                        error_details=error_details,  # Save error details (if any)
                    )

                    # Calculate the date one year ago in IST
                    one_year_ago = ist_timestamp - timedelta(days=365)

                    # Delete logs that are older than one year from the IST timestamp
                    Logs.delete().where(
                        Logs.timestamp < one_year_ago.date()
                    ).execute()

                # Break the loop if successful
                break

            except (IntegrityError, OperationalError) as e:
                # Print error and retry
                print(
                    f"Database logging failed (attempt {attempt + 1}/{retries}): {e}"
                )
                time.sleep(delay)  # Wait before retrying
                delay *= 2  # Exponentially back off the retry delay
            except Exception as e:
                print(f"Unexpected error during logging: {e}")
                break  # Break on unknown errors to avoid infinite retries

        else:
            print("Logging failed after maximum retries.")


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the database log handler
    db_handler = DatabaseLogHandler()
    db_handler.setLevel(logging.DEBUG)

    # Set a formatter to ensure consistent log formatting
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    db_handler.setFormatter(formatter)

    # Add the database handler to the root logger
    logger.addHandler(db_handler)

    return logger
