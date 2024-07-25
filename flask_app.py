from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import json

app = Flask(__name__)
CORS(app)

# Initialize CentralSystem without circular import issue
central_system = None

@app.before_first_request
def initialize_central_system():
    global central_system
    from websocket_server import CentralSystem
    central_system = CentralSystem()

@app.route('/start_transaction', methods=['POST'])
async def start_transaction():
    data = request.json
    charge_point_id = data['charge_point_id']
    id_tag = data['id_tag']
    connector_id = data['connector_id']
    response = await central_system.start_transaction(charge_point_id, id_tag, connector_id)
    return jsonify(response)

@app.route('/stop_transaction', methods=['POST'])
async def stop_transaction():
    data = request.json
    charge_point_id = data['charge_point_id']
    transaction_id = data['transaction_id']
    response = await central_system.stop_transaction(charge_point_id, transaction_id)
    return jsonify(response)

@app.route('/change_availability', methods=['POST'])
async def change_availability():
    data = request.json
    charge_point_id = data['charge_point_id']
    connector_id = data['connector_id']
    type = data['type']
    response = await central_system.change_availability(charge_point_id, connector_id, type)
    return jsonify(response)

@app.route('/change_configuration', methods=['POST'])
async def change_configuration():
    data = request.json
    charge_point_id = data['charge_point_id']
    key = data['key']
    value = data['value']
    response = await central_system.change_configuration(charge_point_id, key, value)
    return jsonify(response)

@app.route('/clear_cache', methods=['POST'])
async def clear_cache():
    data = request.json
    charge_point_id = data['charge_point_id']
    response = await central_system.clear_cache(charge_point_id)
    return jsonify(response)

@app.route('/unlock_connector', methods=['POST'])
async def unlock_connector():
    data = request.json
    charge_point_id = data['charge_point_id']
    connector_id = data['connector_id']
    response = await central_system.unlock_connector(charge_point_id, connector_id)
    return jsonify(response)

@app.route('/get_diagnostics', methods=['POST'])
async def get_diagnostics():
    data = request.json
    charge_point_id = data['charge_point_id']
    location = data['location']
    start_time = data.get('start_time')
    stop_time = data.get('stop_time')
    retries = data.get('retries')
    retry_interval = data.get('retry_interval')
    response = await central_system.get_diagnostics(charge_point_id, location, start_time, stop_time, retries, retry_interval)
    return jsonify(response)

@app.route('/update_firmware', methods=['POST'])
async def update_firmware():
    data = request.json
    charge_point_id = data['charge_point_id']
    location = data['location']
    retrieve_date = data['retrieve_date']
    retries = data.get('retries')
    retry_interval = data.get('retry_interval')
    response = await central_system.update_firmware(charge_point_id, location, retrieve_date, retries, retry_interval)
    return jsonify(response)

@app.route('/reset', methods=['POST'])
async def reset():
    data = request.json
    charge_point_id = data['charge_point_id']
    type = data['type']
    response = await central_system.reset(charge_point_id, type)
    return jsonify(response)

@app.route('/get_status', methods=['GET'])
async def get_status():
    charge_point_id = request.args.get('charge_point_id')
    params = request.args.to_dict()
    response = central_system.get_status(charge_point_id, params)
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
