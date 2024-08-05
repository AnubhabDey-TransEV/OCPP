from flask import Flask, Response, jsonify
from peewee import MySQLDatabase
from datetime import datetime
import json

app = Flask(__name__)

# Configure your MySQL database connection
db = MySQLDatabase(
    'OCPP',  # Replace with your database name
    user='ocpphandler',       # Replace with your database user
    password='ocpp2024',  # Replace with your database password
    host='localhost',       # Replace with your database host if different
    port=3306                # Replace with your database port if different
)

def get_table_columns(table_name):
    """Retrieve column names for a given table."""
    query = db.execute_sql(f"DESCRIBE {table_name}")
    return [row[0] for row in query.fetchall()]

def format_datetime_as_stored(value):
    """Format datetime string as it is stored in the database if it's a string."""
    if isinstance(value, str):
        try:
            # Try to parse the datetime string in the common format
            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # If parsing fails, return the string as-is
            return value
    return value

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return super(DateTimeEncoder, self).default(obj)

@app.before_request
def connect_db():
    """Connect to the database before each request."""
    db.connect()

@app.teardown_request
def close_db(error):
    """Close the database connection after each request."""
    if not db.is_closed():
        db.close()

@app.route('/getCMStoChargerdata', methods=['GET'])
def get_cms_to_charger_data():
    table_name = 'CMS_to_Charger'
    try:
        # Retrieve column names dynamically
        columns = get_table_columns(table_name)
        # Query all data
        query = db.execute_sql(f"SELECT * FROM {table_name}")
        # Build response
        results = []
        for row in query.fetchall():
            row_dict = dict(zip(columns, row))
            # Format datetime fields if they are present
            for col in columns:
                if col in row_dict:
                    row_dict[col] = format_datetime_as_stored(row_dict[col])
            results.append(row_dict)
        # Use json.dumps to serialize with custom encoder
        response = json.dumps(results, cls=DateTimeEncoder)
        return Response(response, mimetype='application/json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/getChargertoCMSdata', methods=['GET'])
def get_charger_to_cms_data():
    table_name = 'Charger_to_CMS'
    try:
        # Retrieve column names dynamically
        columns = get_table_columns(table_name)
        # Query all data
        query = db.execute_sql(f"SELECT * FROM {table_name}")
        # Build response
        results = []
        for row in query.fetchall():
            row_dict = dict(zip(columns, row))
            # Format datetime fields if they are present
            for col in columns:
                if col in row_dict:
                    row_dict[col] = format_datetime_as_stored(row_dict[col])
            results.append(row_dict)
        # Use json.dumps to serialize with custom encoder
        response = json.dumps(results, cls=DateTimeEncoder)
        return Response(response, mimetype='application/json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True)
