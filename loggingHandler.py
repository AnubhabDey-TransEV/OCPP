import logging
from datetime import datetime
from peewee import IntegrityError
from models import Logs

class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        # Avoid recursive logging by not logging the database operations themselves
        if record.name in ('peewee', 'database_logger'):
            return

        try:
            # Prepare the log entry details
            log_entry = self.format(record)
            log_level = record.levelname
            file_origin = record.pathname.split("/")[-1] if "/" in record.pathname else record.pathname.split("\\")[-1]
            timestamp = datetime.fromtimestamp(record.created)
            error_details = record.exc_text if record.exc_info else None

            # Insert the log into the database
            Logs.create(
                log_message=log_entry,
                log_level=log_level,
                timestamp=timestamp,
                file_origin=file_origin,
                error_details=error_details
            )
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
