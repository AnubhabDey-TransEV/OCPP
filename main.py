import asyncio
import json
import logging
import multiprocessing
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, Optional
import httpx
import requests
import uvicorn
import valkey
from decouple import config
from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from peewee import SQL, DoesNotExist, fn
from playhouse.shortcuts import model_to_dict
from pydantic import BaseModel
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
import multiprocessing
from Chargers_to_CMS_Parser import (
    parse_and_store_cancel_reservation_response,
)
from dbconn import keep_db_alive
from models import (
    Analytics,
    Reservation,
    db,
)
from models import (
    OCPPMessageCharger as ChargerToCMS,
)
from models import (
    OCPPMessageCMS as CMSToCharger,
)
from models import (
    Transaction as Transactions,
)
from OCPP_Requests import (
    ChargePoint,
)
from wallet_api_models import (
    DebitWalletRequest,
    EditWalletRequest,
    RechargeWalletRequest,
    UserIDRequest,
)
from wallet_methods import (
    create_wallet_route,
    debit_wallet,
    delete_wallet,
    edit_wallet,
    get_wallet_recharge_history,
    get_wallet_transaction_history,
    recharge_wallet,
)
from fastapi.middleware.gzip import GZipMiddleware
from send_transaction_to_be import start_worker, shutdown_worker
from start_txn_hook_queue import start_hook_worker, shutdown_hook_worker

CHARGER_DATA_KEY = "charger_data_cache"
CACHE_EXPIRY = 7200  # Cache TTL in seconds (2 hours)  # Cache for 2 hours

API_KEY_NAME = "x-api-key"

valkey_uri = config("VALKEY_URI")
valkey_client = valkey.from_url(valkey_uri)


class VerifyAPIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip API key check for WebSocket connections
        if "sec-websocket-key" not in request.headers:
            if request.url.path.startswith("/api/") and request.url.path != "/api/hello":
                api_key = request.headers.get("x-api-key")
                expected_api_key = config("API_KEY")
                logging.info(
                    f"Received API Key: {api_key}, Expected API Key: {expected_api_key}"
                )
                if api_key != expected_api_key:
                    raise HTTPException(status_code=403, detail="Invalid API key")
        response = await call_next(request)
        return response


async def refresh_cache():
    try:
        charger_data = await central_system.get_charger_data()
        valkey_client.setex(
            CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data)
        )  # serialize as JSON string
        print("Charger data has been cached.")
    except Exception as e:
        print(f"Failed to refresh cache: {e}")


# Startup event calling the global refresh_cache function


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Lifespan startup triggered.")
    try:
        await refresh_cache()
        print("✅ refresh_cache() completed.")
        app.state.db_heartbeat_task = asyncio.create_task(keep_db_alive())
        print("🔄 DB heartbeat task started.")
        # app.state.inactivity_watchdog_task = asyncio.create_task(central_system.periodic_inactivity_watchdog())
        # print("Charger inactivity watchdog started.")
        start_worker()
        print("Service to send transaction data to BE started successfully.")
        start_hook_worker()
        print("Service to manageautocutoff data started")

    except Exception as e:
        print(f"❌ Exception during startup: {e}")

    print("🔥 Yielding to start app...")
    yield
    print("🧹 App is shutting down...")

    try:
        valkey_client.delete(CHARGER_DATA_KEY)
        print("🧼 Cache cleared on shutdown.")

        for ws in central_system.active_connections.values():
            try:
                await ws.close(code=1001, reason="Server shutdown")
                print("✅ WebSocket closed on shutdown.")
            except Exception as e:
                print(f"⚠️ Error closing WebSocket during shutdown: {e}")

        try:
            central_system.active_connections.clear()
            print("✅ active_connections cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear active_connections: {e}")

        try:
            central_system.charge_points.clear()
            print("✅ charge_points cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear charge_points: {e}")

        try:
            central_system.frontend_connections.clear()
            print("✅ frontend_connections cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear frontend_connections: {e}")

        try:
            central_system.pending_start_transactions.clear()
            print("✅ pending_start_transactions cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear pending_start_transactions: {e}")

        try:
            central_system.verification_failures.clear()
            print("✅ verification_failures cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear verification_failures: {e}")

        try:
            await shutdown_worker()
            print("Worker to send transaction data to BE shutdown")
        except Exception as e:
            print("Worker to send transaction data to BE couldn't be shut down: {e}")

        try:
            await shutdown_hook_worker()
            print("Worker to manage charging autocutoff shutdown")
        except Exception as e:
            print("Worker to manage charging autocutoff couldn't be shut down: {e}")

    except Exception as e:
        print(f"❌ Error during shutdown: {e}")


middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
    Middleware(VerifyAPIKeyMiddleware),
    Middleware(GZipMiddleware),
]

app = FastAPI(
    middleware=middleware,
    lifespan=lifespan,
)


class WebSocketAdapter:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    async def recv(self):
        return await self.websocket.receive_text()

    async def send(self, message):
        await self.websocket.send_text(message)

    async def close(self):
        await self.websocket.close()


class CentralSystem:
    def __init__(self):
        self.charge_points = {}
        self.active_connections = {}
        self.verification_failures = {}
        self.frontend_connections = {}
        self.pending_start_transactions = {}
        max_workers = int(os.getenv("MAX_CONCURRENT_REQUESTS", multiprocessing.cpu_count() * 3))
        self.send_request_semaphore = asyncio.Semaphore(max_workers)

    def currdatetime(self):
        return datetime.now(timezone.utc)

    async def handle_charge_point(self, websocket: WebSocket, charge_point_id: str):
        # 🛡 Verify charger ID
        if not await self.verify_charger_id(charge_point_id):
            await websocket.close(code=1000)
            return

        # 🔄 Handle reconnect: cleanup previous WS/Valkey state
        if charge_point_id in self.active_connections:
            try:
                await self.active_connections[charge_point_id].close()
                logging.warning(f"[Reconnect] Closed stale WebSocket for charger {charge_point_id}")
            except Exception as e:
                logging.error(f"[Reconnect] Error closing old WebSocket for {charge_point_id}: {e}")

            self.active_connections.pop(charge_point_id, None)
            self.charge_points.pop(charge_point_id, None)

            try:
                valkey_client.delete(f"active_connections:{charge_point_id}")
            except Exception as e:
                logging.warning(f"[Reconnect] Could not delete Valkey key for {charge_point_id}: {e}")

        await websocket.accept()
        logging.info(f"Charge point {charge_point_id} connected.")

        # 💾 Track WebSocket connection
        valkey_client.set(f"active_connections:{charge_point_id}", os.getpid())
        self.active_connections[charge_point_id] = websocket

        # 🧱 Register charge point
        ws_adapter = WebSocketAdapter(websocket)
        charge_point = ChargePoint(charge_point_id, ws_adapter)
        self.charge_points[charge_point_id] = charge_point
        charge_point.online = True

        # 📢 Frontend update
        await self.notify_frontend(charge_point_id, online=True)

        try:
            # 🚀 Start main loop
            start_task = asyncio.create_task(charge_point.start())

            # ⏳ Grace period for charger to get ready
            await asyncio.sleep(10)

            # 🔧 Push configuration
            await self.enforce_remote_only_mode(charge_point_id)

            # 🧘‍♂️ Wait until charger disconnects
            await start_task

        except WebSocketDisconnect:
            logging.info(f"Charge point {charge_point_id} disconnected.")

        except Exception as e:
            logging.error(f"Error occurred while handling charge point {charge_point_id}: {e}")

        finally:
            # 🧹 Always cleanup on disconnect or crash
            valkey_client.delete(f"active_connections:{charge_point_id}")
            self.active_connections.pop(charge_point_id, None)

            if charge_point_id in self.charge_points:
                self.charge_points[charge_point_id].online = False

            await self.notify_frontend(charge_point_id, online=False)
            try:
                await websocket.close()
            except RuntimeError as e:
                if "websocket.close" in str(e):
                    logging.warning(f"[Cleanup] WebSocket for {charge_point_id} already closed.")
                else:
                    logging.error(f"[Cleanup] Error closing WebSocket for {charge_point_id}: {e}")


    async def handle_frontend_websocket(self, websocket: WebSocket, uid: str):
        await websocket.accept()

        # Track the frontend connection
        if uid not in self.frontend_connections:
            self.frontend_connections[uid] = []
        self.frontend_connections[uid].append(websocket)

        # Notify the newly connected frontend about the current status of the charger (online/offline)
        if uid in self.charge_points and self.charge_points[uid].online:
            # Charger is online, notify the frontend immediately
            await self.notify_frontend(uid, online=True)
        else:
            # Charger is offline, notify the frontend immediately
            await self.notify_frontend(uid, online=False)

        try:
            while True:
                # Frontend will just be listening, no need to handle incoming messages
                await websocket.receive_text()

        except WebSocketDisconnect:
            logging.info(f"Frontend WebSocket for charger {uid} disconnected.")

            # Safely remove the WebSocket from the frontend connections list
            if uid in self.frontend_connections:
                self.frontend_connections[uid].remove(websocket)

                # If no other connections are active, remove the entry entirely
                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

        except Exception as e:
            logging.error(f"Error handling frontend WebSocket for charger {uid}: {e}")

            # Ensure the WebSocket is properly removed in case of an error
            if (
                uid in self.frontend_connections
                and websocket in self.frontend_connections[uid]
            ):
                self.frontend_connections[uid].remove(websocket)

                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

    async def notify_frontend(self, charge_point_id: str, online: bool):
        if charge_point_id in self.frontend_connections:
            websockets = list(self.frontend_connections[charge_point_id])
            payload = {
                "charger_id": charge_point_id,
                "status": "Online" if online else "Offline",
            }

            send_tasks = []
            for ws in websockets:
                send_tasks.append(self._safe_send(ws, payload, charge_point_id))

            await asyncio.gather(*send_tasks, return_exceptions=True)
    
    async def _safe_send(self, ws: WebSocket, payload: dict, charge_point_id: str):
        try:
            await ws.send_json(payload)
        except Exception as e:
            logging.error(
                f"Error sending WebSocket message to frontend for charger {charge_point_id}: {e}"
            )

    async def enforce_remote_only_mode(self, charge_point_id: str):
        try:
            # Desired configuration values
            desired_config = {
                "HeartbeatInterval": "15",
                "MeterValueSampleInterval": "15",
                "AuthorizeRemoteTxRequests": "true",
                "LocalAuthorizeOffline": "false",
                "LocalPreAuthorize": "false",
                "AuthorizationCacheEnabled": "false",
                "AllowOfflineTxForUnknownId": "false",
                "StopTransactionOnInvalidId": "true",
                "ChargePointAuthEnable": "true",
                "FreevendEnabled": "false"
            }

            # Fetch current configuration
            current_config_resp = await self.send_request(
                charge_point_id=charge_point_id,
                request_method="get_configuration"
            )

            # Extract config list
            if hasattr(current_config_resp, "configuration_key"):
                raw_config_list = current_config_resp.configuration_key
            elif isinstance(current_config_resp, dict) and "configuration_key" in current_config_resp:
                raw_config_list = current_config_resp["configuration_key"]
            else:
                logging.warning(f"[ConfigSync] Could not parse configuration response from {charge_point_id}")
                return

            # Map current config into dict for comparison
            current_config = {
                entry["key"]: entry.get("value")
                for entry in raw_config_list
                if entry.get("key") is not None
            }

            for key, desired_value in desired_config.items():
                existing_value = current_config.get(key)

                if existing_value is None:
                    logging.warning(f"[ConfigSync] Key '{key}' not found on charger {charge_point_id}, skipping.")
                    continue

                if str(existing_value).strip() == desired_value:
                    logging.info(f"[ConfigSync] '{key}' already correct on {charge_point_id}, skipping.")
                    continue

                logging.info(f"[ConfigSync] Updating '{key}' from '{existing_value}' → '{desired_value}' on {charge_point_id}")

                response = await self.send_request(
                    charge_point_id=charge_point_id,
                    request_method="change_configuration",
                    key=key,
                    value=desired_value
                )

                if hasattr(response, "status") and response.status == "Accepted":
                    logging.info(f"[ConfigSync] Applied {key} = {desired_value} on {charge_point_id}")
                else:
                    logging.warning(
                        f"[ConfigSync] Failed to apply {key} on {charge_point_id}: {getattr(response, 'status', response)}"
                    )
            
            logging.info(f"[ConfigSync] Finished for {charge_point_id}. Final config:")
            for k, v in desired_config.items():
                logging.info(f"  - {k} = {current_config.get(k, 'MISSING')} → intended: {v}")

        except Exception as e:
            logging.error(f"[ConfigSync] Exception during config sync for {charge_point_id}: {e}")


    async def get_charger_data(self):
        """
        Fetch the entire charger data from Valkey cache or API if the cache is not available or expired.
        """
        # Try to get cached charger data from Valkey
        cached_data = valkey_client.get(CHARGER_DATA_KEY)

        if cached_data:
            logging.info("Using cached charger data from Valkey.")
            # Deserialize the JSON string back into a Python object (list)
            return json.loads(cached_data.decode("utf-8"))

        logging.info(
            "Cache not found or expired, fetching fresh charger data from API."
        )
        # Fetch fresh charger data from API
        charger_data = await self.fetch_charger_data_from_api()

        # Serialize the charger data (list) into a JSON string before storing it in Valkey
        charger_data_json = json.dumps(charger_data)

        # Store the data in Valkey with expiration time of 2 hours (7200 seconds)
        valkey_client.setex(CHARGER_DATA_KEY, CACHE_EXPIRY, charger_data_json)

        return charger_data

    async def fetch_charger_data_from_api(self):
        """
        Make an API request to get the charger data from the source.
        """
        first_api_url = config("APICHARGERDATA")
        apiauthkey = config("APIAUTHKEY")
        timeout = 10

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(first_api_url, headers={"apiauthkey": apiauthkey})

        if response.status_code != 200:
            logging.error("Error fetching charger data from API")
            raise HTTPException(
                status_code=500, detail="Error fetching charger data from API"
            )

        charger_data = response.json().get("data", [])
        return charger_data

    # async def check_inactivity_and_update_status(self, charge_point_id: str):
    #     """
    #     Check the last_message_time for the specified charger. If it has been inactive for more than 2 minutes
    #     and is still marked as online, mark it as offline, remove it from active connections,
    #     and close any stale WebSocket connections between the charger and the backend.
    #     """
    #     # Get the charger from the central system's charge_points dictionary
    #     charge_point = self.charge_points.get(charge_point_id)

    #     if not charge_point:
    #         raise HTTPException(
    #             status_code=404, detail=f"Charger {charge_point_id} not found"
    #         )

    #     # Get the current time and the time of the last message
    #     current_time = self.currdatetime()
    #     last_message_time = charge_point.last_message_time

    #     # Calculate the difference in time (in seconds)
    #     time_difference = (datetime.now(timezone.utc) - last_message_time).total_seconds()

    #     # Check if more than 2 minutes (120 seconds) have passed since the last message
    #     if time_difference > 120 and charge_point.online:
    #         # If the charger is marked as online but has been inactive for more than 2 minutes, update its status to offline
    #         charge_point.online = False
    #         charge_point.state["status"] = "Offline"
    #         logging.info(
    #             f"Charger {charge_point_id} has been inactive for more than 2 minutes. Marking it as offline."
    #         )

    #         # Notify the frontend about the charger going offline with updated status
    #         await self.notify_frontend(charge_point_id, online=False)

    #         # Remove the charger from active connections in Valkey
    #         if f"active_connections:{charge_point_id}" in valkey_client:
    #             valkey_client.delete(f"active_connections:{charge_point_id}")
    #             logging.info(
    #                 f"Removed {charge_point_id} from Valkey active connections."
    #             )

    #         # Remove the charger from self.active_connections
    #         # if charge_point_id in self.active_connections:
    #         #     del self.active_connections[charge_point_id]
    #         #     logging.info(
    #         #         f"Removed {charge_point_id} from local active connections."
    #         #     )

    #         # Close the WebSocket connection between the charger and backend if it exists
    #         ws_adapter = (
    #             charge_point.websocket
    #         )  # Assuming ChargePoint has a WebSocket adapter

    #         if ws_adapter:
    #             try:
    #                 # Close the WebSocket connection to the charger
    #                 await ws_adapter.close()
    #                 logging.info(
    #                     f"Closed WebSocket connection for charger {charge_point_id}."
    #                 )
    #             except Exception as e:
    #                 logging.error(f"Error closing WebSocket for {charge_point_id}: {e}")
    #                 if charge_point_id in self.active_connections:
    #                     del self.active_connections[charge_point_id]
    #                     logging.info(
    #                         f"Removed {charge_point_id} from local active connections."
    #                     )

    INACTIVITY_LIMIT = 120  # seconds
    WATCHDOG_INTERVAL = 120     # seconds
    MAX_FAILURE_RETRIES = 3


    async def check_inactivity_and_update_status(self, charge_point_id: str):
        cp = self.charge_points.get(charge_point_id)
        if not cp:
            logging.warning(f"[Watchdog] Charger {charge_point_id} not found.")
            return

        if cp.last_message_time is None:
            return  # never heard from it yet

        now   = datetime.now(timezone.utc)
        delta = (now - cp.last_message_time).total_seconds()

        if delta < INACTIVITY_LIMIT or not cp.online:
            return  # still fine or already offline

        # ---- mark offline ----
        cp.online = False
        cp.state["status"] = "Offline"
        print(f"[Watchdog] {charge_point_id} inactive {int(delta)}s → OFFLINE")

        await self.notify_frontend(charge_point_id, online=False)

        # Valkey cleanup (delete is idempotent)
        try:
            valkey_client.delete(f"active_connections:{charge_point_id}")
        except Exception as e:
            logging.error(f"[Watchdog] Valkey delete failed for {charge_point_id}: {e}")

        # Local WS cleanup
        ws = self.active_connections.pop(charge_point_id, None)
        if ws:
            try:
                await ws.close()
                logging.info(f"[Watchdog] Closed WS for {charge_point_id}")
            except Exception as e:
                logging.error(f"[Watchdog] WS close failed for {charge_point_id}: {e}")

    async def periodic_inactivity_watchdog(self):
        """
        Checks for inactive chargers every 2 minutes.
        Retries up to 3x with exponential backoff.
        """
        failure_counts = {}

        while True:
            try:
                charger_data = await self.get_charger_data()
                known_chargers = {item["uid"] for item in charger_data}
                tasks = []

                for charger_id in known_chargers:
                    async def check(charger_id=charger_id):
                        try:
                            await self.check_inactivity_and_update_status(charger_id)
                            failure_counts.pop(charger_id, None)
                        except Exception as e:
                            count = failure_counts.get(charger_id, 0)
                            if count < MAX_FAILURE_RETRIES:
                                delay = 2 ** count
                                logging.warning(f"[Watchdog] {charger_id} check failed: {e}. Retrying in {delay}s...")
                                failure_counts[charger_id] = count + 1
                                await asyncio.sleep(delay)
                            else:
                                logging.error(f"[Watchdog] {charger_id} failed {MAX_FAILURE_RETRIES} times. Skipping this round.")
                                failure_counts[charger_id] = 0

                    tasks.append(asyncio.create_task(check()))

                await asyncio.gather(*tasks)

                # 🧹 Remove keys for deleted chargers
                failure_counts = {cid: c for cid, c in failure_counts.items() if cid in known_chargers}

            except Exception as e:
                logging.critical(f"[Watchdog] 💀 Global crash in watchdog: {e}")

            await asyncio.sleep(WATCHDOG_INTERVAL)


    async def verify_charger_id(self, charge_point_id: str) -> bool:
        """
        Verify if the charger ID exists in the system by checking cached data first, then the API if necessary.
        If verification fails 3 times, force an update of the cached data.
        """
        # Get the charger data (either from cache or API)
        charger_data = await self.get_charger_data()

        # Check if the charger exists in the cached data
        charger = next(
            (item for item in charger_data if item["uid"] == charge_point_id),
            None,
        )

        if not charger:
            # Track verification failures
            if charge_point_id not in self.verification_failures:
                self.verification_failures[charge_point_id] = 0
            self.verification_failures[charge_point_id] += 1

            logging.error(
                f"Charger with ID {charge_point_id} not found in the system. Verification failed."
            )

            # If verification fails 3 times, force cache update
            if self.verification_failures[charge_point_id] >= 1:
                logging.info(
                    f"Verification failed 3 times for {charge_point_id}, forcing cache update."
                )
                charger_data = await self.fetch_charger_data_from_api()
                valkey_client.setex(
                    CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data)
                )
                self.verification_failures[charge_point_id] = (
                    0  # Reset the failure count
                )

            return False

        return True

    async def send_request(self, charge_point_id, request_method, *args, **kwargs):
        charge_point = self.charge_points.get(charge_point_id)
        if not charge_point:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}

        method = getattr(charge_point, request_method, None)
        if method is None:
            logging.error(
                f"Request method {request_method} not found on ChargePoint {charge_point_id}."
            )
            return {"error": f"Request method {request_method} not found"}

        try:
            async with self.send_request_semaphore:
                response = await method(*args, **kwargs)
            logging.info(
                f"Sent {request_method} to charge point {charge_point_id} with response: {response}"
            )
            return response

        except Exception as e:
            logging.error(
                f"Error sending {request_method} to charge point {charge_point_id}: {e}"
            )
            return {"error": str(e)}

    async def cancel_reservation(self, charge_point_id, reservation_id):
        try:
            reservation = Reservation.get(Reservation.reservation_id == reservation_id)
        except DoesNotExist:
            logging.info(f"No reservation found with ID {reservation_id}")
            return {"error": "Reservation not found"}

        response = await self.send_request(
            charge_point_id=charge_point_id,
            request_method="cancel_reservation",
            reservation_id=reservation_id,
        )

        if response.status == "Accepted":
            reservation.status = "Cancelled"
            reservation.save()

            next_reservation = (
                Reservation.select()
                .where(
                    Reservation.charger_id == charge_point_id,
                    Reservation.status == "Reserved",
                )
                .order_by(Reservation.expiry_date.asc())
                .first()
            )

            if next_reservation:
                logging.info(
                    f"Next reservation for charger {charge_point_id}: {next_reservation.reservation_id}"
                )

            parse_and_store_cancel_reservation_response(
                charge_point_id,
                reservation_id=reservation_id,
                status="Cancelled",
            )
        else:
            parse_and_store_cancel_reservation_response(
                charge_point_id, reservation_id=reservation_id, status="Failed"
            )

        return response

    async def fetch_latest_charger_to_cms_message(self, charge_point_id, message_type):
        """
        Fetch the latest message from the charger-to-CMS table for the given charge point and message type.
        Return the whole entry as JSON, including any dynamic fields.
        """
        try:
            # Use raw SQL to fetch all fields from the table dynamically
            query = """
                SELECT * FROM Charger_to_CMS
                WHERE charger_id = %s AND message_type = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            # Execute the query with the parameters
            cursor = db.execute_sql(query, (charge_point_id, message_type))

            # Fetch the first row
            row = cursor.fetchone()

            # If no row is found, return an error message
            if not row:
                return {"error": "No message received from charger."}

            # Get column names dynamically
            column_names = [desc[0] for desc in cursor.description]

            # Convert the row to a dictionary using column names
            message_data = dict(zip(column_names, row))

            # If there is a payload field and it's JSON, deserialize it
            if "payload" in message_data and message_data["payload"]:
                try:
                    message_data["payload"] = json.loads(message_data["payload"])
                except json.JSONDecodeError:
                    # If it's not valid JSON, leave it as-is
                    pass

            # Return the whole entry as a JSON-compatible dictionary
            return message_data

        except Exception as e:
            logging.error(
                f"Error fetching latest message from charger-to-CMS table: {e}"
            )
            return {"error": str(e)}


# Instantiate the central system
central_system = CentralSystem()


# WebSocket endpoint that supports charger_id with slashes
# WebSocket route for connections with both charger_id and serialnumber
@app.websocket("/{charger_id}/{serialnumber}")
async def websocket_with_serialnumber(
    websocket: WebSocket, charger_id: str, serialnumber: str
):
    logging.info(
        f"Charger {charger_id} with serial number {serialnumber} is connecting."
    )
    await central_system.handle_charge_point(websocket, charger_id)


# WebSocket route for connections with only charger_id
@app.websocket("/{charger_id}")
async def websocket_without_serialnumber(websocket: WebSocket, charger_id: str):
    logging.info(f"Charger {charger_id} is connecting without serial number.")
    await central_system.handle_charge_point(websocket, charger_id)


# WebSocket route for frontend connections
@app.websocket("/frontend/ws/{uid}")
async def frontend_websocket(websocket: WebSocket, uid: str):
    try:
        await central_system.handle_frontend_websocket(websocket, uid)
    except WebSocketDisconnect:
        logging.info(f"Frontend WebSocket for {uid} disconnected.")
    except Exception as e:
        logging.error(f"Error during WebSocket connection for {uid}: {e}")
        await websocket.close(code=1011, reason=f"Error: {e}")


# FastAPI request models
class ChangeAvailabilityRequest(BaseModel):
    uid: str
    connector_id: int
    type: str


class StartTransactionRequest(BaseModel):
    uid: str
    id_tag: str
    connector_id: int


class StopTransactionRequest(BaseModel):
    uid: str
    transaction_id: int


class ChangeConfigurationRequest(BaseModel):
    uid: str
    key: str
    value: str


class UnlockConnectorRequest(BaseModel):
    uid: str
    connector_id: int


class GetDiagnosticsRequest(BaseModel):
    uid: str
    location: str
    start_time: Optional[str]
    stop_time: Optional[str]
    retries: Optional[int]
    retry_interval: Optional[int]


class UpdateFirmwareRequest(BaseModel):
    uid: str
    location: str
    retrieve_date: str
    retries: Optional[int]
    retry_interval: Optional[int]


class ResetRequest(BaseModel):
    uid: str
    type: str


class TriggerMessageRequest(BaseModel):
    uid: str
    requested_message: str


class ReserveNowRequest(BaseModel):
    uid: str
    connector_id: int
    expiry_date: str
    id_tag: str
    reservation_id: int


class CancelReservationRequest(BaseModel):
    uid: str
    reservation_id: int


class GetConfigurationRequest(BaseModel):
    uid: str


class StatusRequest(BaseModel):
    uid: str


class ChargerToCMSQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = (
        None  # Optional dictionary of column name and value pairs for filtering
    )
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering


class CMSToChargerQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = (
        None  # Optional dictionary of column name and value pairs for filtering
    )
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering


class ChargerAnalyticsRequest(BaseModel):
    start_time: Optional[datetime] = (
        None  # Optional start time for the analytics period
    )
    end_time: Optional[datetime] = None  # Optional end time for the analytics period
    charger_id: Optional[str] = None  # Optional filter by charger_id
    include_charger_ids: Optional[bool] = (
        False  # Whether to include the list of unique charger IDs
    )
    user_id: Optional[str] = None


# REST API endpoints


@app.post("/api/hello")
async def hello():
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "Helloo, this is the OCPP HAL API. It is running fine.\n x-auth-sign: `9de5394558b6457186d17e8d7c755faa ||| c94e655d2370a071915e837c72d5eaf4417ecf79b605b4dbadf16363c6fdd206b640ca1d990899372f69b4cfb03baeb5`"}


# Handle OPTIONS for /api/change_availability
@app.options("/api/change_availability")
async def options_change_availability():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/change_availability")
async def change_availability(request: ChangeAvailabilityRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="change_availability",
        connector_id=request.connector_id,
        type=request.type,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "58cfd340a103930ffc6ab9e9458da937 ||| e72b4ad028d01d0efff506d1f9b7abfc63b7e50b04d0d8a2414deee81abe1c6b865a500a70c227306305785202bd4010"
    return {"status": response.status}


# Handle OPTIONS for /api/start_transaction
@app.options("/api/start_transaction")
async def options_start_transaction():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/start_transaction")
async def start_transaction(request: StartTransactionRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="remote_start_transaction",
        id_tag=request.id_tag,
        connector_id=request.connector_id,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "3bbfadec5f71ba88376480edb01611f7 ||| 6f332978e862738763dbcb969aa0f4b2a8ffd84a926c3a87e61312c16b937db48a0c0e5da0b91c786f5dfc70a7aa8936"
    return {"status": response.status}


# Handle OPTIONS for /api/stop_transaction
@app.options("/api/stop_transaction")
async def options_stop_transaction():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


# @app.post("/api/stop_transaction")
# async def stop_transaction(request: StopTransactionRequest):
#     charge_point_id = request.uid

#     response = await central_system.send_request(
#         charge_point_id=charge_point_id,
#         request_method="remote_stop_transaction",
#         transaction_id=request.transaction_id,
#     )
#     if isinstance(response, dict) and "error" in response:
#         raise HTTPException(status_code=404, detail=response["error"])
#     return {"status": response.status}


@app.post("/api/stop_transaction")
async def stop_transaction(request: Request, response_obj: Response):
    data = await request.json()

    # Extract required fields only
    uid = data.get("uid")
    txn_id_str = data.get("transaction_id")

    if not uid or not txn_id_str:
        raise HTTPException(status_code=400, detail="Missing uid or transaction_id")

    # Parse and sanitize transaction_id
    try:
        transaction_id = int(str(txn_id_str).strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid transaction_id format")

    # Proceed as before
    response = await central_system.send_request(
        charge_point_id=uid,
        request_method="remote_stop_transaction",
        transaction_id=transaction_id,
    )

    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    response_obj.headers["x-auth-sign"] = "9a3e77fbddecde2a7a70d338bec3e153 ||| 100b22d441b9f44ddd756ab37a6f71fc3fbd70cf9de25ce5a7b88e9b4f0e4443ad69a91796c4fb3b18b95a2faf1d893d"
    return {"status": response.status}


# Handle OPTIONS for /api/change_configuration
@app.options("/api/change_configuration")
async def options_change_configuration():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/change_configuration")
async def change_configuration(request: ChangeConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="change_configuration",
        key=request.key,
        value=request.value,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "9a3cd3ba0264d2cdc1cc16c3ce349513 ||| a7290f47f85349d0c67a4b8ef61b77f8e0d766a22cd0ee00331a468a51810adcf899f3c88b52e8e23de71ba06e641b6e"
    return {"status": response.status}


# Handle OPTIONS for /api/clear_cache
@app.options("/api/clear_cache")
async def options_clear_cache():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/clear_cache")
async def clear_cache(request: GetConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id, request_method="clear_cache"
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "c11041eb33e1c321b08e326cf7549329 ||| 663205571171c291d2a2a835939e1a8ccfb5e97adfbcc8ee5f3814a61fdb92f79ec25d35fdb18056495cf78e4083f132"
    return {"status": response.status}


# Handle OPTIONS for /api/unlock_connector
@app.options("/api/unlock_connector")
async def options_unlock_connector():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/unlock_connector")
async def unlock_connector(request: UnlockConnectorRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="unlock_connector",
        connector_id=request.connector_id,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "295b919a3cf736194269eaffaaf224f1 ||| 1a2e67f0c18d3b57850b00077708fe1a4fad7b2ba5715de2ee2d0b1a7dcfea78fe5ec3d85abc915e1e5a1c1f8cee07e9"
    return {"status": response.status}


# Handle OPTIONS for /api/get_diagnostics
@app.options("/api/get_diagnostics")
async def options_get_diagnostics():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/get_diagnostics")
async def get_diagnostics(request: GetDiagnosticsRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="get_diagnostics",
        location=request.location,
        start_time=request.start_time,
        stop_time=request.stop_time,
        retries=request.retries,
        retry_interval=request.retry_interval,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "13a603d36b356428a2587fc9da4595bb ||| 21dffa17bc1f04aff1d847c460a9ebafad129daaa2884e1c0672ea8c3067bb7e05ca0ccfef76b37b9ebbba3bce1ba224"
    return {"status": response.status}


# Handle OPTIONS for /api/update_firmware
@app.options("/api/update_firmware")
async def options_update_firmware():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/update_firmware")
async def update_firmware(request: UpdateFirmwareRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="update_firmware",
        location=request.location,
        retrieve_date=request.retrieve_date,
        retries=request.retries,
        retry_interval=request.retry_interval,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "07529c97b04a9748c56c27f1f574322b ||| 4fa082011abe2055e6c6fb56aa6bae3c2d4dc341beb26ddec199f4752f713d08a3733ee7518a5bc7d0a9e1a17f1005df"
    return {"status": response.status}


# Handle OPTIONS for /api/reset
@app.options("/api/reset")
async def options_reset():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/reset")
async def reset(request: ResetRequest, response_obj: Response):
    # charger_serialnum = await central_system.getChargerSerialNum(request.uid)

    # Form the complete charge_point_id
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="reset",
        type=request.type,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "73e3ba1dc113f08b6e9a747e458ab71e ||| f0f858d61885699310ee5a27febb9d3b0b4c2c237e2c307331ca98e44942004b80f6dd397cb352aa6b2bca33e2941a0d"
    return {"status": response.status}


# Handle OPTIONS for /api/get_configuration
@app.options("/api/get_configuration")
async def options_get_configuration():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/get_configuration")
async def get_configuration(request: GetConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id, request_method="get_configuration"
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return response  # Return the configuration response as JSON


# Handle OPTIONS for /api/status
@app.options("/api/status")
async def options_status():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/status")
async def get_charge_point_status(request: StatusRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "1433d8be0fbfd759dfa0f89dc5b07cb5 ||| 17fbdfa25e10ec8774ba91b43431b882e9c43873db8b3495bf8805557bad58b706d47e2bb9ef08e90718e716e8a450f6"
    # Fetch the latest charger data from cache or API
    charger_data = await central_system.get_charger_data()

    # Create a set of active charger IDs from the charger data (API/cache)
    active_chargers = {f"{item['uid']}" for item in charger_data}

    # Handle specific chargers or all_online case
    if request.uid not in ["all", "all_online"]:
        # Form the complete charge_point_id
        charge_point_id = request.uid

        # Ensure the requested charger is still present in the API/cache
        if charge_point_id not in active_chargers:
            raise HTTPException(
                status_code=404, detail="Charger not found in the system"
            )

    else:
        charge_point_id = request.uid

    # Handle "all_online" case
    if charge_point_id == "all_online":
        all_online_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            if (
                charge_point.online and cp_id in active_chargers
            ):  # Only show chargers still present in the API/cache
                online_status = (
                    "Online (with error)" if charge_point.has_error else "Online"
                )
                connectors = charge_point.state["connectors"]
                connectors_status = {}

                for conn_id, conn_state in connectors.items():
                    next_reservation = (
                        Reservation.select()
                        .where(
                            Reservation.charger_id == cp_id,
                            Reservation.connector_id == conn_id,
                            Reservation.status == "Reserved",
                        )
                        .order_by(Reservation.expiry_date.asc())
                        .dicts()
                        .first()
                    )

                    if next_reservation:
                        next_reservation_info = {
                            "reservation_id": next_reservation["reservation_id"],
                            "connector_id": next_reservation["connector_id"],
                            "from_time": next_reservation["from_time"],
                            "to_time": next_reservation["to_time"],
                        }
                    else:
                        next_reservation_info = None

                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "latest_meter_value": conn_state.get("last_meter_value"),
                        "latest_transaction_consumption_kwh": conn_state.get(
                            "last_transaction_consumption_kwh", 0
                        ),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "latest_transaction_id": conn_state.get("transaction_id"),
                        "next_reservation": next_reservation_info,
                    }

                all_online_statuses[cp_id] = {
                    "status": charge_point.state["status"],
                    "connectors": connectors_status,
                    "online": online_status,
                    "latest_message_received_time": charge_point.last_message_time.isoformat(),
                }
        return all_online_statuses

    # Handle "all" case - return all chargers (online and offline) present in the API/cache
    if charge_point_id == "all":
        all_statuses = {}

        # Iterate through the charger data (from API/cache)
        for charger in charger_data:
            # Get the charger ID from the API/cache
            charger_id = charger[
                "uid"
            ]  # Use the 'uid' field from the charger data to set charger_id
            charge_point = central_system.charge_points.get(charger_id)
            online_status = "Offline"  # Default to offline unless active

            connectors_status = {}

            # If the charge point is active, use its real-time status and data
            if charge_point:
                online_status = (
                    "Online (with error)"
                    if charge_point.online and charge_point.has_error
                    else "Online"
                )
                connectors = charge_point.state["connectors"]

                for conn_id, conn_state in connectors.items():
                    next_reservation = (
                        Reservation.select()
                        .where(
                            Reservation.charger_id == charger_id,
                            Reservation.connector_id == conn_id,
                            Reservation.status == "Reserved",
                        )
                        .order_by(Reservation.expiry_date.asc())
                        .dicts()
                        .first()
                    )

                    if next_reservation:
                        next_reservation_info = {
                            "reservation_id": next_reservation["reservation_id"],
                            "connector_id": next_reservation["connector_id"],
                            "from_time": next_reservation["from_time"],
                            "to_time": next_reservation["to_time"],
                        }
                    else:
                        next_reservation_info = None

                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "last_meter_value": conn_state.get("last_meter_value"),
                        "last_transaction_consumption_kwh": conn_state.get(
                            "last_transaction_consumption_kwh", 0
                        ),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "transaction_id": conn_state.get("transaction_id"),
                        "next_reservation": next_reservation_info,
                    }

            # If the charger is offline, use its last known data from charge_points
            elif charger_id in central_system.charge_points:
                charge_point = central_system.charge_points[charger_id]
                online_status = "Offline"
                connectors = charge_point.state["connectors"]

                for conn_id, conn_state in connectors.items():
                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "last_meter_value": conn_state.get("last_meter_value"),
                        "last_transaction_consumption_kwh": conn_state.get(
                            "last_transaction_consumption_kwh", 0
                        ),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "transaction_id": conn_state.get("transaction_id"),
                    }

            all_statuses[charger_id] = {
                "status": (charge_point.state["status"] if charge_point else "Offline"),
                "connectors": connectors_status,
                "online": online_status,
                "last_message_received_time": (
                    charge_point.last_message_time.isoformat() if charge_point else None
                ),
            }

        return all_statuses

    # Handle specific charge point status request (for specific chargers)
    if charge_point_id:
        charge_point = central_system.charge_points.get(charge_point_id)
        if not charge_point:
            raise HTTPException(status_code=404, detail="Charge point not found")

        online_status = (
            "Online (with error)"
            if charge_point.online and charge_point.has_error
            else "Online"
            if charge_point.online
            else "Offline"
        )
        connectors = charge_point.state["connectors"]
        connectors_status = {}

        for conn_id, conn_state in connectors.items():
            next_reservation = (
                Reservation.select()
                .where(
                    Reservation.charger_id == charge_point_id,
                    Reservation.connector_id == conn_id,
                    Reservation.status == "Reserved",
                )
                .order_by(Reservation.expiry_date.asc())
                .dicts()
                .first()
            )

            if next_reservation:
                next_reservation_info = {
                    "reservation_id": next_reservation["reservation_id"],
                    "connector_id": next_reservation["connector_id"],
                    "from_time": next_reservation["from_time"],
                    "to_time": next_reservation["to_time"],
                }
            else:
                next_reservation_info = None

            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get(
                    "last_transaction_consumption_kwh", 0
                ),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
                "next_reservation": next_reservation_info,
            }

        return {
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat(),
        }

    # Handle specific charge point status request (this block will handle only specific charge points, not "all")
    if charge_point_id and charge_point_id != "all":
        charge_point = central_system.charge_points.get(charge_point_id)
        if not charge_point:
            raise HTTPException(status_code=404, detail="Charge point not found")

        online_status = (
            "Online (with error)"
            if charge_point.online and charge_point.has_error
            else "Online"
            if charge_point.online
            else "Offline"
        )
        connectors = charge_point.state["connectors"]
        connectors_status = {}

        for conn_id, conn_state in connectors.items():
            next_reservation = (
                Reservation.select()
                .where(
                    Reservation.charger_id == charge_point_id,
                    Reservation.connector_id == conn_id,
                    Reservation.status == "Reserved",
                )
                .order_by(Reservation.expiry_date.asc())
                .dicts()
                .first()
            )

            if next_reservation:
                next_reservation_info = {
                    "reservation_id": next_reservation["reservation_id"],
                    "connector_id": next_reservation["connector_id"],
                    "from_time": next_reservation["from_time"],
                    "to_time": next_reservation["to_time"],
                }
            else:
                next_reservation_info = None

            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get(
                    "last_transaction_consumption_kwh", 0
                ),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
                "next_reservation": next_reservation_info,
            }

        return {
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat(),
        }


# Handle OPTIONS for /api/trigger_message
@app.options("/api/trigger_message")
async def options_trigger_message():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/trigger_message")
async def trigger_message(request: TriggerMessageRequest, response_obj: Response):
    charge_point_id = request.uid
    requested_message = (
        request.requested_message
    )  # Assuming you want this as the message type

    # Send the trigger message
    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="trigger_message",
        requested_message=requested_message,
    )

    # Check the type of `response` to ensure it's the expected type
    if hasattr(response, "status"):
        status = response.status
    else:
        raise HTTPException(status_code=500, detail="Failed to trigger message")

    await asyncio.sleep(6)

    # After sending, fetch the latest message from the database
    latest_message = await central_system.fetch_latest_charger_to_cms_message(
        charge_point_id, requested_message
    )

    response_obj.headers["x-auth-sign"] = "42361b8da509b97843f231761b86e143 ||| b442624f883ff0967929ea79258303f412081e7b4c02161a83bed8be7dd2c7de2d24314078c72a50a3084da3746f6f1e"

    return {"status": status, "latest_message": latest_message}


# Handle OPTIONS for /api/reserve_now
@app.options("/api/reserve_now")
async def options_reserve_now():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/reserve_now")
async def reserve_now(request: ReserveNowRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="reserve_now",
        connector_id=request.connector_id,
        expiry_date=request.expiry_date,
        id_tag=request.id_tag,
        reservation_id=request.reservation_id,
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "0911a54182106a3ac2f92f1ff1546d79 ||| d1570f4f7f3840421a98d94c5fc5ef3464b3994da6d8f7ffef754fb685a1b3413d4e0a72409578dbff1969cf0cb3a6c4"
    return {"status": response.status}


# Handle OPTIONS for /api/cancel_reservation
@app.options("/api/cancel_reservation")
async def options_cancel_reservation():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/cancel_reservation")
async def cancel_reservation(request: CancelReservationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.cancel_reservation(
        charge_point_id=charge_point_id, reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    response_obj.headers["x-auth-sign"] = "ac69a3cfac05203af15f6251d1dd61bb ||| e8ca16481f8115ce69de4d9e1c89a32dae300a3bf11c3d4bc23af465f7b6b70091957ab810d5c30b78aa8698ac7273ab"
    return {"status": response.status}


# Handle OPTIONS for /api/query_charger_to_cms
@app.options("/api/query_charger_to_cms")
async def options_query_charger_to_cms():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/query_charger_to_cms")
async def query_charger_to_cms(request: ChargerToCMSQueryRequest, response_obj: Response):
    query = ChargerToCMS.select()

    if request.uid:
        query = query.where(ChargerToCMS.charger_id == request.uid)

    if request.filters:
        for column, value in request.filters.items():
            if not ChargerToCMS._meta.fields.get(column):
                raise HTTPException(
                    status_code=404,
                    detail=f"Column '{column}' not found in Charger_to_CMS table.",
                )
            query = query.where(getattr(ChargerToCMS, column) == value)

    if request.start_time:
        query = query.where(ChargerToCMS.timestamp >= request.start_time)
    if request.end_time:
        query = query.where(ChargerToCMS.timestamp <= request.end_time)

    if request.limit:
        start, end = map(int, request.limit.split("-"))
        query = query.order_by(ChargerToCMS.id).limit(end - start + 1).offset(start - 1)
    else:
        query = query.order_by(ChargerToCMS.id)

    results = [model_to_dict(row) for row in query]
    response_obj.headers["x-auth-sign"] = "311b77385928c1a3ff0b16b8ffb0736d ||| 8a4dcbd1742ea947d16cbcf39fa7393e3fad84cdffbcf011a9866e4e640328a0dc35ca4dd9f1255358718b6c0108fcd4"
    return {"data": results}


# Handle OPTIONS for /api/query_cms_to_charger
@app.options("/api/query_cms_to_charger")
async def options_query_cms_to_charger():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/query_cms_to_charger")
async def query_cms_to_charger(request: CMSToChargerQueryRequest, response_obj: Response):
    query = CMSToCharger.select()

    if request.uid:
        query = query.where(CMSToCharger.charger_id == request.uid)

    if request.filters:
        for column, value in request.filters.items():
            if not CMSToCharger._meta.fields.get(column):
                raise HTTPException(
                    status_code=404,
                    detail=f"Column '{column}' not found in CMS_to_Charger table.",
                )
            query = query.where(getattr(CMSToCharger, column) == value)

    if request.start_time:
        query = query.where(CMSToCharger.timestamp >= request.start_time)
    if request.end_time:
        query = query.where(CMSToCharger.timestamp <= request.end_time)

    if request.limit:
        start, end = map(int, request.limit.split("-"))
        query = query.order_by(CMSToCharger.id).limit(end - start + 1).offset(start - 1)
    else:
        query = query.order_by(CMSToCharger.id)

    results = [model_to_dict(row) for row in query]
    response_obj.headers["x-auth-sign"] = "b948d4542ad4d2a33b12a079610b0b89 ||| 064eeda5c1903483612c95469fd8b24901bfcbfc796da9f8af0ef1d98924052c1086afbfbeb7081e42adf28a3fa05480"
    return {"data": results}


# Handle OPTIONS for /api/query_transactions
@app.options("/api/query_transactions")
async def options_query_transactions():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/query_transactions")
async def query_transactions(request: ChargerToCMSQueryRequest, response_obj: Response):
    query = Transactions.select()

    if request.uid:
        query = query.where(Transactions.charger_id == request.uid)

    if request.filters:
        for column, value in request.filters.items():
            if not Transactions._meta.fields.get(column):
                raise HTTPException(
                    status_code=404,
                    detail=f"Column '{column}' not found in transactions table.",
                )
            query = query.where(getattr(Transactions, column) == value)

    if request.start_time:
        query = query.where(Transactions.start_time >= request.start_time)
    if request.end_time:
        query = query.where(Transactions.stop_time <= request.end_time)

    if request.limit:
        start, end = map(int, request.limit.split("-"))
        query = query.order_by(Transactions.id).limit(end - start + 1).offset(start - 1)
    else:
        query = query.order_by(Transactions.id)

    results = [model_to_dict(row) for row in query]
    response_obj.headers["x-auth-sign"] = "70d8f676bc4e0192183f3fd7b1a71c90 ||| 9f4e9b4e81bd03dd7a06daeaf5c36123b80c3bcab2bd2a40d815ac223157f7c320db1adf20176482ee8b198003dc63ce"
    return {"data": results}


# Handle OPTIONS for /api/query_reservations
@app.options("/api/query_reservations")
async def options_query_reservations():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/query_reservations")
async def query_reservations(request: ChargerToCMSQueryRequest, response_obj: Response):
    query = Reservation.select()

    if request.uid:
        query = query.where(Reservation.charger_id == request.uid)

    if request.filters:
        for column, value in request.filters.items():
            if not Reservation._meta.fields.get(column):
                raise HTTPException(
                    status_code=404,
                    detail=f"Column '{column}' not found in reservations table.",
                )
            query = query.where(getattr(Reservation, column) == value)

    if request.start_time:
        query = query.where(Reservation.reserved_at >= request.start_time)
    if request.end_time:
        query = query.where(Reservation.reserved_at <= request.end_time)

    if request.limit:
        start, end = map(int, request.limit.split("-"))
        query = query.order_by(Reservation.id).limit(end - start + 1).offset(start - 1)
    else:
        query = query.order_by(Reservation.id)

    results = [model_to_dict(row) for row in query]
    response_obj.headers["x-auth-sign"] = "50bedff2f46bf1edec9ba8c9a10125e5 ||| 448bebb76d8a7a17fe6ff62bbadfc59e136ac78c8f4df3ac918d52b2dbb282889c7123336cfad6da69b374f9386d477a"
    return {"data": results}


def format_duration(seconds):
    """Convert seconds into a human-readable format: years, days, hours, minutes, seconds."""
    years, remainder = divmod(seconds, 31536000)  # 365 * 24 * 60 * 60
    days, remainder = divmod(remainder, 86400)  # 24 * 60 * 60
    hours, remainder = divmod(remainder, 3600)  # 60 * 60
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if years > 0:
        parts.append(f"{int(years)} years")
    if days > 0:
        parts.append(f"{int(days)} days")
    if hours > 0:
        parts.append(f"{int(hours)} hours")
    if minutes > 0:
        parts.append(f"{int(minutes)} minutes")
    if seconds > 0:
        parts.append(f"{int(seconds)} seconds")

    return ", ".join(parts) if parts else "0 seconds"


@app.post("/api/charger_analytics")
async def charger_analytics(request: ChargerAnalyticsRequest, response_obj: Response):
    # Query to get the earliest and latest timestamps from your data tables
    min_max_time_query = ChargerToCMS.select(
        fn.MIN(ChargerToCMS.timestamp).alias("min_time"),
        fn.MAX(ChargerToCMS.timestamp).alias("max_time"),
    ).first()
    min_time = min_max_time_query.min_time
    max_time = min_max_time_query.max_time

    # Use the earliest and latest timestamps if not provided in the request
    start_time = request.start_time or (
        min_time
        if min_time
        else datetime.min.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
    )
    end_time = request.end_time or (
        max_time
        if max_time
        else datetime.max.replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=0)
    )

    # Initialize variables to store the results
    analytics = {}

    # Initialize variables for cumulative logic
    total_cumulative_uptime_seconds = 0
    total_cumulative_transactions = 0
    total_cumulative_electricity_used = 0
    total_cumulative_time_occupied_seconds = 0

    charger_ids = []

    # Step 1: Handle case when charger_id == "cumulative" (fetch all chargers for user)
    if request.charger_id == "cumulative":
        user_id = request.user_id  # Assuming user_id is passed in the body

        # API call to fetch the chargers for the specific user
        api_url = config("APIUSERCHARGERDATA")  # API URL from the config
        apiauthkey = config("APIAUTHKEY")  # API Auth key from config

        # Post request to fetch user charger data
        response = requests.post(
            api_url,
            headers={"apiauthkey": apiauthkey},
            json={"get_user_id": user_id},
            timeout=10,
        )

        if response.status_code != 200:
            return {"error": "Failed to fetch user charger data from external API"}

        user_charger_data = response.json().get("user_chargerunit_details", [])
        charger_ids = [charger["uid"] for charger in user_charger_data]

        # If no chargers found for the user, return null values
        if not charger_ids:
            return {
                "total_uptime": "0 seconds",
                "total_transactions": 0,
                "total_electricity_used_kwh": 0,
                "message": "No chargers registered for this user.",
            }
    else:
        # When a specific charger_id is provided, use that charger
        charger_ids = [request.charger_id]  # For a single charger

    # Step 2: Calculate the uptime for each charger in the list
    for charger_id in charger_ids:
        uptime_query = (
            ChargerToCMS.select(ChargerToCMS.charger_id, ChargerToCMS.timestamp)
            .where(
                (ChargerToCMS.timestamp.between(start_time, end_time))
                & (ChargerToCMS.charger_id == charger_id)
            )
            .order_by(ChargerToCMS.charger_id, ChargerToCMS.timestamp)
        )

        uptime_results = list(uptime_query)

        # If no data (messages) is found, set the uptime to zero for this charger and continue to the next
        if not uptime_results:
            analytics[charger_id] = {
                "total_uptime": "0 seconds",
                "uptime_percentage": 0,
                "total_transactions": 0,
                "total_electricity_used_kwh": 0,
                "occupancy_rate_percentage": 0,
                "average_session_duration": "0 seconds",
                "peak_usage_times": "No data available",
            }
            continue  # Skip further processing for this charger

        # Initialize for each charger, ensuring no stale data is carried over
        charger_uptime_data = {
            "total_uptime_seconds": 0,
            "total_possible_uptime_seconds": (end_time - start_time).total_seconds(),
            "total_time_occupied_seconds": 0,  # Initialize to 0
            "session_durations": [],
            "peak_usage_times": [0] * 24,  # 24-hour time slots
        }

        previous_timestamp = None
        first_message = None

        for row in uptime_results:
            charger_id, timestamp = row.charger_id, row.timestamp

            if not first_message:
                first_message = timestamp

            if previous_timestamp:
                time_difference = (timestamp - previous_timestamp).total_seconds()

                if time_difference <= 30:
                    # If the time difference is 30 seconds or less, accumulate the uptime
                    charger_uptime_data["total_uptime_seconds"] += time_difference
                else:
                    # Charger was offline for more than 30 seconds, skip adding to uptime
                    logging.info(
                        f"Charger {charger_id} was offline for {time_difference - 30} seconds."
                    )

            # Update the previous timestamp for the next iteration
            previous_timestamp = timestamp

        # Step 3: Calculate total number of transactions, electricity used, and session-related metrics
        transaction_summary_query = (
            Transactions.select(
                fn.COUNT(Transactions.id).alias("total_transactions"),
                fn.SUM(Transactions.total_consumption).alias("total_electricity_used"),
                fn.SUM(
                    fn.TIMESTAMPDIFF(
                        SQL("SECOND"),
                        Transactions.start_time,
                        Transactions.stop_time,
                    )
                ).alias("total_time_occupied"),
            )
            .where(
                (Transactions.start_time.between(start_time, end_time))
                & (Transactions.charger_id == charger_id)
            )
            .first()
        )

        total_transactions = transaction_summary_query.total_transactions
        total_electricity_used = transaction_summary_query.total_electricity_used or 0.0
        total_time_occupied = (
            transaction_summary_query.total_time_occupied or 0
        )  # Ensure total_time_occupied is not None

        # Add the current charger's metrics to the cumulative values (only if "cumulative" is requested)
        if request.charger_id == "cumulative":
            total_cumulative_uptime_seconds += charger_uptime_data[
                "total_uptime_seconds"
            ]
            total_cumulative_transactions += total_transactions or 0
            total_cumulative_electricity_used += total_electricity_used
            total_cumulative_time_occupied_seconds += total_time_occupied or 0

        # Calculate session durations and peak usage times for each charger
        session_query = Transactions.select(
            Transactions.start_time,
            fn.TIMESTAMPDIFF(
                SQL("SECOND"), Transactions.start_time, Transactions.stop_time
            ).alias("session_duration"),
        ).where(
            (Transactions.start_time.between(start_time, end_time))
            & (Transactions.charger_id == charger_id)
        )

        sessions = list(session_query)

        for session in sessions:
            start_time = session.start_time
            session_duration = session.session_duration
            charger_uptime_data["session_durations"].append(session_duration)
            hour_of_day = start_time.hour
            charger_uptime_data["peak_usage_times"][hour_of_day] += 1

        # Calculate peak usage hours
        max_usage_count = max(charger_uptime_data["peak_usage_times"])
        peak_usage_hours = [
            f"{hour}:00 - {hour + 1}:00"
            for hour, count in enumerate(charger_uptime_data["peak_usage_times"])
            if count == max_usage_count
        ]

        if max_usage_count == 0:
            peak_usage_hours = [
                "No peak usage times - charger was not used during this period."
            ]

        # Compile the results for this charger
        uptime_seconds = charger_uptime_data["total_uptime_seconds"]
        uptime_percentage = (
            round(
                (uptime_seconds / charger_uptime_data["total_possible_uptime_seconds"])
                * 100,
                3,
            )
            if uptime_seconds > 0
            else 0
        )
        average_session_duration_seconds = (
            sum(charger_uptime_data["session_durations"]) / total_transactions
            if total_transactions > 0
            else 0
        )
        occupancy_rate = (
            round(
                (
                    total_time_occupied
                    / charger_uptime_data["total_possible_uptime_seconds"]
                )
                * 100,
                3,
            )
            if total_time_occupied > 0
            else 0
        )

        analytics_data = {
            "charger_id": charger_id,
            "timestamp": self.currdatetime(),
            "total_uptime": format_duration(uptime_seconds),
            "uptime_percentage": uptime_percentage,
            "total_transactions": total_transactions,
            "total_electricity_used_kwh": total_electricity_used,
            "occupancy_rate_percentage": occupancy_rate,
            "average_session_duration": format_duration(
                average_session_duration_seconds
            ),
            "peak_usage_times": ", ".join(
                peak_usage_hours
            ),  # Store as a comma-separated string
        }

        # Save analytics data to the database
        Analytics.create(**analytics_data)

        # Add the analytics data to the response
        analytics[charger_id] = analytics_data

    # Step 4: Return cumulative analytics if 'cumulative' is requested
    if request.charger_id == "cumulative":
        return {
            "total_uptime": format_duration(total_cumulative_uptime_seconds),
            "total_transactions": total_cumulative_transactions,
            "total_electricity_used_kwh": total_cumulative_electricity_used,
            "total_time_occupied": format_duration(
                total_cumulative_time_occupied_seconds
            ),
        }

    # Step 5: Return individual charger analytics
    response_obj.headers["x-auth-sign"] = "639010f6105f6a136a8bbfaf4967fe47 ||| f67927aa5805f975d80cf5ad32798c9442e3c251934a135ce6d4f1eaf8cc5a6d5b332ed4c830e09064e93842be2e846d"
    return {"analytics": analytics}


@app.post("/api/check_charger_inactivity")
async def check_charger_inactivity(request: StatusRequest, response_obj: Response):
    """
    API endpoint that allows the frontend to check if a charger has been inactive for more than 2 minutes.
    If the charger has been inactive and is still marked as online, it will update the status to offline.
    """
    charge_point_id = request.uid

    # Call the method to check inactivity and update the status if necessary
    result = await central_system.check_inactivity_and_update_status(charge_point_id)

    response_obj.headers["x-auth-sign"] = "cf14e1b39d45fa052941bd79eaa13e06 ||| 3c79f80d1b0838ac28c258711c0b20d231c65118ee1be434bc0d1d71182451564dbf46fdd3b38386af76169e4fdbb9a4"
    return result


@app.post("/api/wallet/create")
async def create_wallet_for_user(user: UserIDRequest, response_obj: Response):
    api_url = config("APICHARGERDATA")  # Load API URL from .env
    apiauthkey = config("APIAUTHKEY")  # Load API Auth key from .env

    # Call the wallet creation logic from wallet_methods.py using user_id from the request body
    response_obj.headers["x-auth-sign"] = "e4455a817728fbe3e48241a88b94444c ||| c864e0606dac091ada84e9daaaa45362da4b4cf3f8f8e00c99c05b27f73c93517964e723ccf2b8dd40f860727c06e2a7"
    return await create_wallet_route(api_url, apiauthkey, user.user_id)


@app.post("/api/wallet/delete")
async def delete_wallet_data(request: UserIDRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "b4419195e6a02bd90fee123894de19ce ||| 84c5595bf063c83f6e16ab9416cc69eb541e40a7193337c56444faea999848aea9c83bad05b96281b9839f2f222f0ec8"
    return await delete_wallet(request.user_id)


@app.post("/api/wallet/recharge")
async def recharge_wallet_route(request: RechargeWalletRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "1dc21fd0daff176bc7a34d64da83d02f ||| d7f15c1c8c285cb19fbbe7989a7c7b2c4d5cf05135fd1dc2f311878722549f733388239aa31263f2b9100b4a0ab72302"
    return await recharge_wallet(request.user_id, request.recharge_amount)


@app.post("/api/wallet/edit")
async def edit_wallet_route(request: EditWalletRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "8248c223057e044579738ad6ea2e40d1 ||| 6ea00c1826d34d863974a44f59ac69f6490155da01d4d02da99a95b219ecd3a6bdc49fe3f03298082253ac98baa5e0a8"
    return await edit_wallet(request.user_id, request.balance)


@app.post("/api/wallet/debit")
async def debit_wallet_route(request: DebitWalletRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "0f9e5fac79e3c5de5040dec96028dcdf ||| 0cb000188757558244c14daef5d25447aa6d4577855b1e8b5a41762befa193c8652d7a273f58c2dac62b2d21f24144d0"
    return await debit_wallet(request.user_id, request.debit_amount)


@app.post("/api/wallet/recharge-history")
async def get_recharge_history_route(request: UserIDRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "52d4169a6ce5e536845d75cfe3c2a8e9 ||| 8b1ecd970ed6d810bf6edb4176c108c116cd7ad281d1561e0bbe5a8d1c680080c376e10b2cab2bd4c55dd6748ca32bb3"
    return await get_wallet_recharge_history(request.user_id)


@app.post("/api/wallet/transaction-history")
async def get_transaction_history_route(request: UserIDRequest, response_obj: Response):
    response_obj.headers["x-auth-sign"] = "2abb23f726a318e9a463fa5784e826c5 ||| d7b49e072e330047a10a63abf8a0dca4d7fde7052ce70da95111761074e2cb77add137a64a9b9347645accca825e7712"
    return await get_wallet_transaction_history(request.user_id)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=config("F_SERVER_HOST", default="127.0.0.1"),
        port=int(config("F_SERVER_PORT", default=8050)),
        proxy_headers=True,
        reload=False,  # set True in dev
    )
