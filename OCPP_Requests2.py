import asyncio
import logging
from datetime import datetime, timezone as dt_timezone
from pytz import timezone
from ocpp.v201 import call, call_result, ChargePoint as CP
from ocpp.routing import on
import Chargers_to_CMS_Parser_2 as parser_c2c
import CMS_to_Charger_Parser_2 as parser_ctc
from models import Transaction


class ChargePoint(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_id = id  # Store Charger_ID
        self.websocket = websocket  # Store WebSocket instance
        self.state = {"status": "Inactive", "connectors": {}}
        self.online = False  # Track if the charger is online
        self.has_error = False  # Track if the charger has an error
        self.last_message_time = (
            datetime.now()
        )  # Timestamp of the last received message

    def currdatetime(self):
        curr = datetime.now()
        return curr.isoformat()

    def update_connector_state(
        self,
        connector_id,
        status=None,
        meter_value=None,
        error_code=None,
        transaction_id=None,
    ):
        if connector_id not in self.state["connectors"]:
            self.state["connectors"][connector_id] = {
                "status": "Unknown",
                "last_meter_value": None,
                "last_transaction_consumption_kwh": 0.0,  # Track last transaction consumption per connector
                "error_code": "NoError",
                "transaction_id": None,
            }
        if status:
            self.state["connectors"][connector_id]["status"] = status
        if meter_value is not None:
            if (
                self.state["connectors"][connector_id]["transaction_id"]
                != transaction_id
            ):
                initial_meter_value = self.state["connectors"][connector_id][
                    "last_meter_value"
                ]
                if initial_meter_value and meter_value:
                    consumption_kwh = (
                        meter_value - initial_meter_value
                    ) / 1000.0  # Convert Wh to kWh
                    self.state["connectors"][connector_id][
                        "last_transaction_consumption_kwh"
                    ] = consumption_kwh
                    logging.info(
                        f"Connector {connector_id} last transaction consumed {consumption_kwh:.3f} kWh."
                    )
            self.state["connectors"][connector_id][
                "last_meter_value"
            ] = meter_value
        if error_code:
            self.state["connectors"][connector_id]["error_code"] = error_code
        if transaction_id is not None:
            self.state["connectors"][connector_id][
                "transaction_id"
            ] = transaction_id

        # Check for errors
        if error_code and error_code != "NoError":
            self.has_error = True
        else:
            self.has_error = False

    async def send(self, message):
        response = await super().send(message)
        return response

    async def start(self):
        await super().start()

    def update_transaction_id(self, connector_id, transaction_id):
        if transaction_id is not None:
            self.update_connector_state(
                connector_id, transaction_id=transaction_id
            )

    # Inbound messages from chargers to CMS
    @on("Authorize")
    async def on_authorize(self, **kwargs):
        logging.debug(f"Received Authorize request with kwargs: {kwargs}")

        id_token = kwargs.get("id_token")

        is_authorized = await self.validate_id_token(id_token)

        status = "Accepted" if is_authorized else "Invalid"

        parser_c2c.parse_and_store_authorize(
            self.charger_id, id_token=id_token, status=status
        )

        response = call_result.Authorize(id_token_info={"status": status})

        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "Authorize",
            "Authorize",
            self.currdatetime(),
            status=status,
        )
        return response

    @on("BootNotification")
    async def on_boot_notification(self, **kwargs):
        logging.debug(f"Received BootNotification with kwargs: {kwargs}")
        parser_c2c.parse_and_store_boot_notification(self.charger_id, **kwargs)

        response = call_result.BootNotification(
            current_time=self.currdatetime(), interval=900, status="Accepted"
        )
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "BootNotification",
            "BootNotification",
            self.currdatetime(),
            status="Accepted",
        )
        return response

    @on("Heartbeat")
    async def on_heartbeat(self, **kwargs):
        logging.debug(f"Received Heartbeat with kwargs: {kwargs}")
        parser_c2c.parse_and_store_heartbeat(self.charger_id, **kwargs)

        response = call_result.Heartbeat(current_time=self.currdatetime())
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id, "Heartbeat", "Heartbeat", self.currdatetime()
        )
        return response

    @on("TransactionEvent")
    async def on_transaction_event(self, **kwargs):
        logging.debug(f"Received TransactionEvent with kwargs: {kwargs}")
        event_type = kwargs.get("event_type")
        connector_id = kwargs.get("evse_id")
        transaction_id = kwargs.get("transaction_id")

        if event_type == "Started":
            meter_start = kwargs.get("meter_start")
            id_token = kwargs.get("id_token")

            parser_c2c.parse_and_store_transaction_event(
                self.charger_id, **kwargs
            )

            transaction_record = Transaction.create(
                charger_id=self.charger_id,
                connector_id=connector_id,
                meter_start=meter_start,
                start_time=datetime.now(),
                id_tag=id_token,
            )
            self.update_transaction_id(connector_id, transaction_id)
            self.state["connectors"][connector_id][
                "transaction_record_id"
            ] = transaction_record.id
            self.state["status"] = "Active"
            self.update_connector_state(
                connector_id,
                status="Charging",
                meter_value=meter_start,
                transaction_id=transaction_id,
            )

        elif event_type == "Ended":
            meter_stop = kwargs.get("meter_stop")

            transaction_record_id = self.state["connectors"][connector_id].get(
                "transaction_record_id"
            )
            if transaction_record_id:
                transaction_record = Transaction.get_by_id(
                    transaction_record_id
                )
                transaction_record.meter_stop = meter_stop
                transaction_record.stop_time = datetime.now()
                transaction_record.total_consumption = (
                    meter_stop - transaction_record.meter_start
                )
                transaction_record.save()

            parser_c2c.parse_and_store_transaction_event(
                self.charger_id, **kwargs
            )
            self.state["status"] = "Inactive"
            self.update_connector_state(
                connector_id,
                status="Available",
                meter_value=meter_stop,
                transaction_id=None,
            )

        response = call_result.TransactionEvent()
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "TransactionEvent",
            "TransactionEvent",
            self.currdatetime(),
            connector_id=connector_id,
        )
        return response

    @on("StatusNotification")
    async def on_status_notification(self, **kwargs):
        logging.debug(f"Received StatusNotification with kwargs: {kwargs}")
        error_code = kwargs.get("error_code", "NoError")
        connector_id = kwargs.get("evse_id")
        status = kwargs.get("status")
        transaction_id = kwargs.get("transaction_id")
        parser_c2c.parse_and_store_status_notification(
            self.charger_id, **kwargs
        )

        self.update_connector_state(
            connector_id,
            status=status,
            error_code=error_code,
            transaction_id=transaction_id,
        )

        response = call_result.StatusNotification()
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "StatusNotification",
            "StatusNotification",
            self.currdatetime(),
            connector_id=connector_id,
        )
        return response

    @on(
        "NotifyMonitoringReport"
    )  # OCPP v2.0.1 equivalent of DiagnosticsStatusNotification
    async def on_notify_monitoring_report(self, **kwargs):
        logging.debug(f"Received NotifyMonitoringReport with kwargs: {kwargs}")
        parser_c2c.parse_and_store_diagnostics_status(
            self.charger_id, **kwargs
        )

        response = call_result.NotifyMonitoringReport()
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "NotifyMonitoringReport",
            "NotifyMonitoringReport",
            self.currdatetime(),
        )
        return response

    @on("FirmwareStatusNotification")
    async def on_firmware_status_notification(self, **kwargs):
        logging.debug(
            f"Received FirmwareStatusNotification with kwargs: {kwargs}"
        )
        parser_c2c.parse_and_store_firmware_status(self.charger_id, **kwargs)

        response = call_result.FirmwareStatusNotification()
        parser_ctc.parse_and_store_acknowledgment(
            self.charger_id,
            "FirmwareStatusNotification",
            "FirmwareStatusNotification",
            self.currdatetime(),
        )
        return response

    # Outbound messages from CMS to Charger
    async def remote_start_transaction(self, id_token, connector_id):
        logging.debug(
            f"Sending RemoteStartTransaction: id_token {id_token}, connector_id {connector_id}"
        )
        request = call.RemoteStartTransaction(
            id_token=id_token,
            evse_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"RemoteStartTransaction response: {response}")

        transaction_id = (
            response.transaction_id
            if hasattr(response, "transaction_id")
            else None
        )
        self.update_transaction_id(connector_id, transaction_id)

        parser_ctc.parse_and_store_remote_start_transaction(
            self.charger_id, id_token=id_token, connector_id=connector_id
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "RemoteStartTransaction",
            "RemoteStartTransaction",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(
            f"Sending RemoteStopTransaction: transaction_id {transaction_id}"
        )
        request = call.RemoteStopTransaction(transaction_id=transaction_id)

        response = await self.call(request)
        logging.debug(f"RemoteStopTransaction response: {response}")

        connector_id = next(
            (
                k
                for k, v in self.state["connectors"].items()
                if v["transaction_id"] == transaction_id
            ),
            None,
        )
        if connector_id:
            self.update_connector_state(connector_id, transaction_id=None)

        parser_ctc.parse_and_store_remote_stop_transaction(
            self.charger_id, transaction_id=transaction_id
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "RemoteStopTransaction",
            "RemoteStopTransaction",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def change_availability(self, evse_id, operational_status):
        logging.debug(
            f"Sending ChangeAvailability: evse_id {evse_id}, operational_status {operational_status}"
        )
        request = call.ChangeAvailability(
            evse_id=evse_id,
            operational_status=operational_status,
        )

        response = await self.call(request)
        logging.debug(f"ChangeAvailability response: {response}")

        parser_ctc.parse_and_store_change_availability(
            self.charger_id,
            evse_id=evse_id,
            operational_status=operational_status,
            status=response.status,
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "ChangeAvailability",
            "ChangeAvailability",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.SetVariables(
            set_variable_data=[
                {
                    "attribute_type": "Actual",  # OCPP v2.0.1 requires defining the attribute type
                    "component": {"name": "Connector"},
                    "variable": {"name": key},
                    "attribute_value": value,
                }
            ]
        )

        response = await self.call(request)
        logging.debug(f"SetVariables response: {response}")

        parser_ctc.parse_and_store_change_configuration(
            self.charger_id,
            key=key,
            value=value,
            status=response.set_variable_result[0].attribute_status,
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "SetVariables",
            "SetVariables",
            self.currdatetime(),
            status=response.set_variable_result[0].attribute_status,
        )
        return response

    async def get_configuration(self, component: str, variable: str = None):
        logging.debug(
            f"Sending GetConfiguration for component: {component}, variable: {variable}"
        )

        # Construct the GetVariables request data
        get_variable_data = [{"component": {"name": component}}]

        # If a variable is provided, include it in the request
        if variable:
            get_variable_data[0]["variable"] = {"name": variable}

        request = call.GetVariables(get_variable_data=get_variable_data)

        # Store the get configuration request in the parser
        parser_ctc.parse_and_store_get_configuration(self.charger_id)

        try:
            # Send the request
            response = await self.call(request)
            logging.debug(f"GetVariables response: {response}")

            # Log the results of the GetVariables response
            if hasattr(response, "get_variable_result"):
                for result in response.get_variable_result:
                    logging.debug(
                        f"Component: {result.component.name}, Variable: {result.variable.name}, "
                        f"Value: {result.attribute_value}, Status: {result.attribute_status}"
                    )

            # Store the response in the parser
            parser_c2c.parse_and_store_get_configuration_response(
                self.charger_id, response.get_variable_result
            )

            return response

        except Exception as e:
            logging.error(f"Error in get_configuration: {e}")
            raise

    async def clear_cache(self):
        logging.debug(f"Sending ClearCache")
        request = call.ClearCache()

        response = await self.call(request)
        logging.debug(f"ClearCache response: {response}")

        parser_ctc.parse_and_store_clear_cache(
            self.charger_id, status=response.status
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "ClearCache",
            "ClearCache",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def unlock_connector(self, evse_id):
        logging.debug(f"Sending UnlockConnector: evse_id {evse_id}")
        request = call.UnlockConnector(evse_id=evse_id)

        response = await self.call(request)
        logging.debug(f"UnlockConnector response: {response}")

        parser_ctc.parse_and_store_unlock_connector(
            self.charger_id, evse_id=evse_id, status=response.status
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "UnlockConnector",
            "UnlockConnector",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def get_diagnostics(
        self,
        location,
        start_time=None,
        stop_time=None,
        retries=None,
        retry_interval=None,
    ):
        logging.debug(
            f"Sending GetDiagnostics: location {location}, start_time={start_time}, stop_time={stop_time}, retries={retries}, retry_interval={retry_interval}"
        )
        request = call.GetDiagnostics(
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"GetDiagnostics response: {response}")

        parser_ctc.parse_and_store_get_diagnostics(
            self.charger_id,
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
            status=response.status,
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "GetDiagnostics",
            "GetDiagnostics",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def update_firmware(
        self, location, retrieve_date, retries=None, retry_interval=None
    ):
        logging.debug(
            f"Sending UpdateFirmware: location {location}, retrieve_date {retrieve_date}, retries={retries}, retry_interval={retry_interval}"
        )
        request = call.UpdateFirmware(
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"UpdateFirmware response: {response}")

        parser_ctc.parse_and_store_update_firmware(
            self.charger_id,
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
            status=response.status,
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "UpdateFirmware",
            "UpdateFirmware",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.Reset(
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"Reset response: {response}")

        parser_ctc.parse_and_store_reset(
            self.charger_id, type=type, status=response.status
        )
        parser_c2c.parse_and_store_acknowledgment(
            self.charger_id,
            "Reset",
            "Reset",
            self.currdatetime(),
            status=response.status,
        )
        return response

    async def trigger_message(self, requested_message):
        request = call.TriggerMessage(requested_message=requested_message)
        parser_ctc.parse_and_store_trigger_message(
            self.charger_id, requested_message=requested_message
        )
        response = await self.call(request)
        return response

    async def reserve_now(
        self, evse_id, expiry_date, id_token, reservation_id
    ):
        request = call.ReserveNow(
            evse_id=evse_id,
            expiry_date=expiry_date,
            id_token=id_token,
            reservation_id=reservation_id,
        )
        parser_ctc.parse_and_store_reserve_now(
            self.charger_id,
            evse_id=evse_id,
            expiry_date=expiry_date,
            id_token=id_token,
            reservation_id=reservation_id,
        )
        response = await self.call(request)
        parser_c2c.parse_and_store_reserve_now_response(
            self.charger_id,
            evse_id=evse_id,
            expiry_date=expiry_date,
            id_token=id_token,
            reservation_id=reservation_id,
            status=response.status,
        )
        return response

    async def cancel_reservation(self, reservation_id):
        request = call.CancelReservation(reservation_id=reservation_id)
        parser_ctc.parse_and_store_cancel_reservation(
            self.charger_id, reservation_id=reservation_id
        )
        response = await self.call(request)
        parser_c2c.parse_and_store_cancel_reservation_response(
            self.charger_id,
            reservation_id=reservation_id,
            status=response.status,
        )
        return response
