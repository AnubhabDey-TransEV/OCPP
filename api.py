from flask import Flask, request, jsonify
import asyncio
import logging
from websockets.exceptions import ConnectionClosedError
from websocket_server import CentralSystem
import threading
from models import Reservation

app = Flask(__name__)
central_system = CentralSystem()

# Start the WebSocket server in a separate thread
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
server_thread = threading.Thread(target=loop.run_until_complete, args=(central_system.server(),))
server_thread.start()

def run_async(coroutine):
    """
    Run the given coroutine in the asyncio event loop running in another thread.
    
    Parameters:
    coroutine (coroutine): The coroutine to run.
    
    Returns:
    The result of the coroutine.
    """
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

@app.route("/get_configuration", methods=["POST"])
def get_configuration():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    logging.debug(f"Received request to get configuration: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='get_configuration'
            )
        )
        logging.debug(f"Response from get_configuration: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify(response)  # Return the configuration response as JSON
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
    
    if charge_point_id == "all_online":
        all_online_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            if charge_point.online:
                online_status = "Online (with error)" if charge_point.has_error else "Online"
                connectors = charge_point.state["connectors"]
                connectors_status = {}

                for conn_id, conn_state in connectors.items():
                    # Find the next valid reservation for this connector
                    next_reservation = Reservation.select().where(
                        Reservation.charger_id == cp_id,
                        Reservation.connector_id == conn_id,
                        Reservation.status == 'Reserved'
                    ).order_by(Reservation.expiry_date.asc()).dicts().first()

                    if next_reservation:
                        next_reservation_info = {
                            "reservation_id": next_reservation['reservation_id'],
                            "connector_id": next_reservation['connector_id'],
                            "from_time": next_reservation['from_time'],
                            "to_time": next_reservation['to_time']
                        }
                    else:
                        next_reservation_info = None

                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "latest_meter_value": conn_state.get("last_meter_value"),
                        "latest_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "latest_transaction_id": conn_state.get("transaction_id"),
                        "next_reservation": next_reservation_info
                    }

                all_online_statuses[cp_id] = {
                    "status": charge_point.state["status"],
                    "connectors": connectors_status,
                    "online": online_status,
                    "latest_message_received_time": charge_point.last_message_time.isoformat()
                }
        return jsonify(all_online_statuses)

    if charge_point_id:
        charge_point = central_system.charge_points.get(charge_point_id)
        if not charge_point:
            return jsonify({"error": "Charge point not found"}), 404

        online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online" if charge_point.online else "Offline"
        connectors = charge_point.state["connectors"]
        connectors_status = {}

        for conn_id, conn_state in connectors.items():
            # Find the next valid reservation for this connector
            next_reservation = Reservation.select().where(
                Reservation.charger_id == charge_point_id,
                Reservation.connector_id == conn_id,
                Reservation.status == 'Reserved'
            ).order_by(Reservation.expiry_date.asc()).dicts().first()

            if next_reservation:
                next_reservation_info = {
                    "reservation_id": next_reservation['reservation_id'],
                    "connector_id": next_reservation['connector_id'],
                    "from_time": next_reservation['from_time'],
                    "to_time": next_reservation['to_time']
                }
            else:
                next_reservation_info = None

            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
                "next_reservation": next_reservation_info
            }

        return jsonify({
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat()
        })
    else:
        # Return the current state of all connected charge points
        all_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online" if charge_point.online else "Offline"
            connectors = charge_point.state["connectors"]
            connectors_status = {}

            for conn_id, conn_state in connectors.items():
                # Find the next valid reservation for this connector
                next_reservation = Reservation.select().where(
                    Reservation.charger_id == cp_id,
                    Reservation.connector_id == conn_id,
                    Reservation.status == 'Reserved'
                ).order_by(Reservation.expiry_date.asc()).dicts().first()

                if next_reservation:
                    next_reservation_info = {
                        "reservation_id": next_reservation['reservation_id'],
                        "connector_id": next_reservation['connector_id'],
                        "from_time": next_reservation['from_time'],
                        "to_time": next_reservation['to_time']
                    }
                else:
                    next_reservation_info = None

                connectors_status[conn_id] = {
                    "status": conn_state["status"],
                    "last_meter_value": conn_state.get("last_meter_value"),
                    "last_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                    "error_code": conn_state.get("error_code", "NoError"),
                    "transaction_id": conn_state.get("transaction_id"),
                    "next_reservation": next_reservation_info
                }

            all_statuses[cp_id] = {
                "status": charge_point.state["status"],
                "connectors": connectors_status,
                "online": online_status,
                "last_message_received_time": charge_point.last_message_time.isoformat()
            }
        return jsonify(all_statuses)

@app.route("/trigger_message", methods=["POST"])
def trigger_message():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    requested_message = data.get('requested_message')
    logging.debug(f"Received request to trigger message: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='trigger_message',
                requested_message=requested_message
            )
        )
        logging.debug(f"Response from trigger_message: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/reserve_now", methods=["POST"])
def reserve_now():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    connector_id = data.get('connector_id')
    expiry_date = data.get('expiry_date')
    id_tag = data.get('id_tag')
    reservation_id = data.get('reservation_id')
    logging.debug(f"Received request to reserve now: {data}")
    try:
        response = run_async(
            central_system.send_request(
                charge_point_id=charge_point_id,
                request_method='reserve_now',
                connector_id=connector_id,
                expiry_date=expiry_date,
                id_tag=id_tag,
                reservation_id=reservation_id
            )
        )
        logging.debug(f"Response from reserve_now: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route("/cancel_reservation", methods=["POST"])
def cancel_reservation():
    data = request.json
    charge_point_id = data.get('charge_point_id')
    reservation_id = data.get('reservation_id')
    logging.debug(f"Received request to cancel reservation: {data}")
    try:
        response = run_async(
            central_system.cancel_reservation(
                charge_point_id=charge_point_id,
                reservation_id=reservation_id
            )
        )
        logging.debug(f"Response from cancel_reservation: {response}")
        if isinstance(response, dict) and "error" in response:
            return jsonify(response), 404
        return jsonify({"status": response.status})
    except ConnectionClosedError as e:
        logging.error(f"Connection error: {e}")
        return jsonify({"error": f"Connection error: {e}"}), 500
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
