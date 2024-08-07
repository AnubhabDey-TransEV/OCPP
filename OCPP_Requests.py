import asyncio
import logging
from datetime import datetime, timezone as dt_timezone
from pytz import timezone
from ocpp.v16 import call, call_result, ChargePoint as CP
from ocpp.routing import on
import Chargers_to_CMS_Parser as parser_c2c
import CMS_to_Charger_Parser as parser_ctc

logging.basicConfig(level=logging.DEBUG)

class ChargePoint(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_id = id  # Store Charger_ID
        self.state = {
            "status": "Inactive",
            "connectors": {}
        }

    def currdatetime(self):
        utc_time = datetime.now(dt_timezone.utc)
        ist = timezone('Asia/Kolkata')
        ist_time = utc_time.astimezone(ist)
        return ist_time.isoformat()
    
    def update_connector_state(self, connector_id, status=None, meter_value=None, error_code=None, transaction_id=None):
        if connector_id not in self.state["connectors"]:
            self.state["connectors"][connector_id] = {
                "status": "Unknown",
                "last_meter_value": None,
                "error_code": "NoError",
                "transaction_id": None
            }
        if status:
            self.state["connectors"][connector_id]["status"] = status
        if meter_value is not None:
            self.state["connectors"][connector_id]["last_meter_value"] = meter_value
        if error_code:
            self.state["connectors"][connector_id]["error_code"] = error_code
        if transaction_id:
            self.state["connectors"][connector_id]["transaction_id"] = transaction_id
            
    # Inbound messages from chargers to CMS
    @on('BootNotification')
    async def on_boot_notification(self, **kwargs):
        logging.debug(f"Received BootNotification with kwargs: {kwargs}")
        parser_c2c.parse_and_store_boot_notification(self.charger_id, **kwargs)

        response = call_result.BootNotification(
            current_time=self.currdatetime(),
            interval=900,
            status='Accepted'
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "BootNotification", "BootNotification", self.currdatetime(), status='Accepted')
        return response

    @on('Heartbeat')
    async def on_heartbeat(self, **kwargs):
        logging.debug(f"Received Heartbeat with kwargs: {kwargs}")
        parser_c2c.parse_and_store_heartbeat(self.charger_id, **kwargs)

        response = call_result.Heartbeat(current_time=self.currdatetime())
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "Heartbeat", "Heartbeat", self.currdatetime())
        return response

    @on('StartTransaction')
    async def on_start_transaction(self, **kwargs):
        logging.debug(f"Received StartTransaction with kwargs: {kwargs}")
        transaction_id = kwargs.get('transaction_id')
        connector_id = kwargs.get('connector_id')
        meter_start = kwargs.get('meter_start')
        parser_c2c.parse_and_store_start_transaction(self.charger_id, **kwargs)

        self.state["status"] = "Active"
        self.update_connector_state(connector_id, status="Charging", meter_value=meter_start, transaction_id=transaction_id)

        response = call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={
                'status': 'Accepted'
            }
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StartTransaction", "StartTransaction", self.currdatetime(), connector_id=connector_id, transaction_id=transaction_id, status='Accepted')
        return response

    @on('StopTransaction')
    async def on_stop_transaction(self, **kwargs):
        logging.debug(f"Received StopTransaction with kwargs: {kwargs}")
        connector_id = kwargs.get('connector_id')
        transaction_id = kwargs.get('transaction_id')
        meter_stop = kwargs.get('meter_stop')
        parser_c2c.parse_and_store_stop_transaction(self.charger_id, **kwargs)

        self.state["status"] = "Inactive"
        self.update_connector_state(connector_id, status="Available", meter_value=meter_stop, transaction_id=transaction_id)

        response = call_result.StopTransaction(
            id_tag_info={
                'status': 'Accepted'
            }
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StopTransaction", "StopTransaction", self.currdatetime(), connector_id=connector_id, transaction_id=transaction_id, status='Accepted')
        return response

    @on('MeterValues')
    async def on_meter_values(self, **kwargs):
        logging.debug(f"Received MeterValues with kwargs: {kwargs}")
        connector_id = kwargs.get('connector_id')
        meter_values = kwargs.get('meter_value', [])
        parser_c2c.parse_and_store_meter_values(self.charger_id, **kwargs)

        if meter_values:
            last_meter_value = meter_values[-1]
            self.update_connector_state(connector_id, meter_value=last_meter_value)

        response = call_result.MeterValues()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "MeterValues", "MeterValues", self.currdatetime(), connector_id=connector_id)
        return response

    @on('StatusNotification')
    async def on_status_notification(self, **kwargs):
        logging.debug(f"Received StatusNotification with kwargs: {kwargs}")
        connector_id = kwargs.get('connector_id')
        status = kwargs.get('status')
        error_code = kwargs.get('error_code', 'NoError')
        parser_c2c.parse_and_store_status_notification(self.charger_id, **kwargs)

        self.update_connector_state(connector_id, status=status, error_code=error_code)

        response = call_result.StatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StatusNotification", "StatusNotification", self.currdatetime(), connector_id=connector_id)
        return response

    @on('DiagnosticsStatusNotification')
    async def on_diagnostics_status_notification(self, **kwargs):
        logging.debug(f"Received DiagnosticsStatusNotification with kwargs: {kwargs}")
        parser_c2c.parse_and_store_diagnostics_status(self.charger_id, **kwargs)

        response = call_result.DiagnosticsStatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "DiagnosticsStatusNotification", "DiagnosticsStatusNotification", self.currdatetime())
        return response

    @on('FirmwareStatusNotification')
    async def on_firmware_status_notification(self, **kwargs):
        logging.debug(f"Received FirmwareStatusNotification with kwargs: {kwargs}")
        parser_c2c.parse_and_store_firmware_status(self.charger_id, **kwargs)

        response = call_result.FirmwareStatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "FirmwareStatusNotification", "FirmwareStatusNotification", self.currdatetime())
        return response

    # Outbound messages from CMS to Charger
    async def remote_start_transaction(self, id_tag, connector_id):
        logging.debug(f"Sending RemoteStartTransaction: id_tag {id_tag}, connector_id {connector_id}")
        request = call.RemoteStartTransaction(
            id_tag=id_tag,
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"RemoteStartTransaction response: {response}")

        parser_ctc.parse_and_store_remote_start_transaction(self.charger_id, id_tag=id_tag, connector_id=connector_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "RemoteStartTransaction", "RemoteStartTransaction", self.currdatetime(), status=response.status)
        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(f"Sending RemoteStopTransaction: transaction_id {transaction_id}")
        request = call.RemoteStopTransaction(transaction_id=transaction_id)

        response = await self.call(request)
        logging.debug(f"RemoteStopTransaction response: {response}")

        parser_ctc.parse_and_store_remote_stop_transaction(self.charger_id, transaction_id=transaction_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "RemoteStopTransaction", "RemoteStopTransaction", self.currdatetime(), status=response.status)
        return response

    async def change_availability(self, connector_id, type):
        logging.debug(f"Sending ChangeAvailability: connector_id {connector_id}, type {type}")
        request = call.ChangeAvailability(
            connector_id=connector_id,
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"ChangeAvailability response: {response}")

        parser_ctc.parse_and_store_change_availability(self.charger_id, connector_id=connector_id, type=type, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ChangeAvailability", "ChangeAvailability", self.currdatetime(), status=response.status)
        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.ChangeConfiguration(
            key=key,
            value=value,
        )

        response = await self.call(request)
        logging.debug(f"ChangeConfiguration response: {response}")

        parser_ctc.parse_and_store_change_configuration(self.charger_id, key=key, value=value, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ChangeConfiguration", "ChangeConfiguration", self.currdatetime(), status=response.status)
        return response

    async def get_configuration(self):
        logging.debug(f"Sending GetConfiguration:")
        request = call.GetConfiguration()

        parser_ctc.parse_and_store_get_configuration(self.charger_id)
        try:
            response = await self.call(request)
            logging.debug(f"GetConfiguration response: {response}")

            # Log the types and values of the configuration keys in the response
            if hasattr(response, 'configuration_key'):
                logging.debug(f"Configuration keys: {response.configuration_key}")
                for config in response.configuration_key:
                    logging.debug(f"Key: {config.get('key')} Type: {type(config.get('key'))}")
                    logging.debug(f"Value: {config.get('value')} Type: {type(config.get('value'))}")
                    logging.debug(f"Readonly: {config.get('readonly')} Type: {type(config.get('readonly'))}")

            parser_c2c.parse_and_store_get_configuration_response(self.charger_id, response.configuration_key)
            return response
        except Exception as e:
            logging.error(f"Error in get_configuration: {e}")
            raise

    async def clear_cache(self):
        logging.debug(f"Sending ClearCache")
        request = call.ClearCache()

        response = await self.call(request)
        logging.debug(f"ClearCache response: {response}")

        parser_ctc.parse_and_store_clear_cache(self.charger_id, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ClearCache", "ClearCache", self.currdatetime(), status=response.status)
        return response

    async def unlock_connector(self, connector_id):
        logging.debug(f"Sending UnlockConnector: connector_id {connector_id}")
        request = call.UnlockConnector(
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"UnlockConnector response: {response}")

        parser_ctc.parse_and_store_unlock_connector(self.charger_id, connector_id=connector_id, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "UnlockConnector", "UnlockConnector", self.currdatetime(), status=response.status)
        return response

    async def get_diagnostics(self, location, start_time=None, stop_time=None, retries=None, retry_interval=None):
        logging.debug(f"Sending GetDiagnostics: location {location}, start_time={start_time}, stop_time={stop_time}, retries={retries}, retry_interval={retry_interval}")
        request = call.GetDiagnostics(
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"GetDiagnostics response: {response}")

        parser_ctc.parse_and_store_get_diagnostics(self.charger_id, location=location, start_time=start_time, stop_time=stop_time, retries=retries, retry_interval=retry_interval, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "GetDiagnostics", "GetDiagnostics", self.currdatetime(), status=response.status)
        return response

    async def update_firmware(self, location, retrieve_date, retries=None, retry_interval=None):
        logging.debug(f"Sending UpdateFirmware: location {location}, retrieve_date {retrieve_date}, retries={retries}, retry_interval={retry_interval}")
        request = call.UpdateFirmware(
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"UpdateFirmware response: {response}")

        parser_ctc.parse_and_store_update_firmware(self.charger_id, location=location, retrieve_date=retrieve_date, retries=retries, retry_interval=retry_interval, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "UpdateFirmware", "UpdateFirmware", self.currdatetime(), status=response.status)
        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.Reset(
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"Reset response: {response}")

        parser_ctc.parse_and_store_reset(self.charger_id, type=type, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "Reset", "Reset", self.currdatetime(), status=response.status)
        return response

    async def get_meter_values(self, connector_id):
        logging.debug(f"Sending GetMeterValues: connector_id {connector_id}")
        request = call.GetMeterValues(
            connector_id=connector_id
        )

        response = await self.call(request)
        logging.debug(f"GetMeterValues response: {response}")

        parser_ctc.parse_and_store_get_meter_values(self.charger_id, connector_id=connector_id, status=response.status)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "GetMeterValues", "GetMeterValues", self.currdatetime(), status=response.status)
        return response
