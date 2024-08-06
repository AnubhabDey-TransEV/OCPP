from flask import Flask, request, jsonify
import asyncio
import logging
from websockets.exceptions import ConnectionClosedError
from websocket_server import CentralSystem
import threading

app = Flask(__name__)
central_system = CentralSystem()

# Start the WebSocket server in a separate thread
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
server_thread = threading.Thread(target=loop.run_until_complete, args=(central_system.server(),))
server_thread.start()

def run_async(coroutine):
    future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    return future.result()

@app.route("/change_availability", methods=["POST"])
def change_availability():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    connector_id = data.get('connector_id')
    type = data.get('type')
    logging.debug(f"Received request to change availability: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='change_availability',
                connector_id=connector_id,
                type=type
            )
        )
        logging.debug(f"Response from change_availability: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/start_transaction", methods=["POST"])
def start_transaction():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    id_tag = data.get('id_tag')
    connector_id = data.get('connector_id')
    logging.debug(f"Received request to start transaction: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='remote_start_transaction',
                id_tag=id_tag,
                connector_id=connector_id
            )
        )
        logging.debug(f"Response from start_transaction: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/stop_transaction", methods=["POST"])
def stop_transaction():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    transaction_id = data.get('transaction_id')
    logging.debug(f"Received request to stop transaction: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='remote_stop_transaction',
                transaction_id=transaction_id
            )
        )
        logging.debug(f"Response from stop_transaction: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/change_configuration", methods=["POST"])
def change_configuration():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    key = data.get('key')
    value = data.get('value')
    logging.debug(f"Received request to change configuration: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='change_configuration',
                key=key,
                value=value
            )
        )
        logging.debug(f"Response from change_configuration: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/clear_cache", methods=["POST"])
def clear_cache():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    logging.debug(f"Received request to clear cache: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='clear_cache'
            )
        )
        logging.debug(f"Response from clear_cache: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/unlock_connector", methods=["POST"])
def unlock_connector():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    connector_id = data.get('connector_id')
    logging.debug(f"Received request to unlock connector: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='unlock_connector',
                connector_id=connector_id
            )
        )
        logging.debug(f"Response from unlock_connector: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/get_diagnostics", methods=["POST"])
def get_diagnostics():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    location = data.get('location')
    start_time = data.get('start_time')
    stop_time = data.get('stop_time')
    retries = data.get('retries')
    retry_interval = data.get('retry_interval')
    logging.debug(f"Received request to get diagnostics: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='get_diagnostics',
                location=location,
                start_time=start_time,
                stop_time=stop_time,
                retries=retries,
                retry_interval=retry_interval
            )
        )
        logging.debug(f"Response from get_diagnostics: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/update_firmware", methods=["POST"])
def update_firmware():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    location = data.get('location')
    retrieve_date = data.get('retrieve_date')
    retries = data.get('retries')
    retry_interval = data.get('retry_interval')
    logging.debug(f"Received request to update firmware: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='update_firmware',
                location=location,
                retrieve_date=retrieve_date,
                retries=retries,
                retry_interval=retry_interval
            )
        )
        logging.debug(f"Response from update_firmware: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/reset", methods=["POST"])
def reset():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    type = data.get('type')
    logging.debug(f"Received request to reset: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='reset',
                type=type
            )
        )
        logging.debug(f"Response from reset: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/get_meter_values", methods=["POST"])
def get_meter_values():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    connector_id = data.get('connector_id')
    logging.debug(f"Received request to get meter values: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='get_meter_values',
                connector_id=connector_id
            )
        )
        logging.debug(f"Response from get_meter_values: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/status", methods=["POST"])
def get_charge_point_status():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    charge_point = central_system.charge_points.get(charge_point_id)
    if not charge_point:
        return jsonify({"error": "Charge point not found"}), 404

    # Return the current state of the charge point
    return jsonify({
        "charger_id": charge_point_id,
        "status": charge_point.state["status"],
        "connectors": charge_point.state["connectors"]
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
