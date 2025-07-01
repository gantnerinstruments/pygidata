import os
from dataclasses import dataclass
from typing import Dict, Any

import websocket
import threading
import json
import logging
from enum import Enum
import ssl
import certifi
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


message_counter = 0
lock = threading.Lock()

def message_count_logger():
    global message_counter
    while True:
        time.sleep(1)
        with lock:
            count = message_counter
            message_counter = 0
        logging.info(f"Messages received in last second: {count}")

# Start background logging thread
threading.Thread(target=message_count_logger, daemon=True).start()


# Enums translated from TypeScript
class GInsWSWorkerTypes(Enum):
    NotSpecified = None
    WSWorkerType_NotSpecified = 0
    WSWorkerType_XmlRpcCall = 1
    WSWorkerType_OnlineData = 2
    WSWorkerType_Authentication = 13
    WSWorkerType_SystemState = 15
    WSWorkerType_MessageEvents = 17


class GInsWSMessageTypes(Enum):
    WSMsgType_NotSpecified = 0
    WSMsgType_Subscribe = 1
    WSMsgType_Publish = 2
    WSMsgType_Stop = 3
    WSMsgType_Start = 4
    WSMsgType_Reconfigure = 5
    WSMsgType_Destroy = 6
    WSMsgType_Request = 7
    WSMsgType_Response = 8
    WSMsgType_Identify = 9
    WSMsgType_Error = 10
    WSMsgType_End = 11
    WSMsgType_NotRoutable = 12
    WSMsgType_Authentication = 13


class GInsWSWorkerMessageFormat(Enum):
    WSMsgFormat_JSON = 0
    WSMsgFormat_BINARY = 1
    WSMsgFormat_STRING = 2
    WSMsgFormat_UNKNOWN = 3


LOCAL_URL = "localhost"

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
WS_LOCATION = os.getenv("WS_LOCATION")

IS_LOCAL_TEST_VERSION = True


@dataclass
class WSWorkerComponent:
    id: int
    worker_type: GInsWSWorkerTypes
    config: Dict[str, Any]
    route: str = ""  # Default to empty if not provided


# Placeholder classes for types and dependencies
class Worker:
    def __init__(self):
        self.subscribers = []

    def add_subscriber(self, subscriber):
        self.subscribers.append(subscriber)


class SocketService:
    def __init__(self, ws):
        self.ws = ws

    def _send_message(self, header: list, payload: dict):
        version = b"\x00"
        header_json = json.dumps(header, separators=(",", ":"))
        payload_json = json.dumps(payload)
        message = (
            version
            + len(header_json.encode("utf-8")).to_bytes(2, "little")
            + (header_json + payload_json).encode("utf-8")
        )
        self.ws.send(message, opcode=websocket.ABNF.OPCODE_BINARY)

    def subscribe(self, worker_id, worker_type, payload, route=None, worker_add=None):

        message_header = [
            route if route else "",  # Route
            GInsWSMessageTypes.WSMsgType_Subscribe.value,  # MessageType
            worker_type.value,  # WorkerType
            worker_id,  # WorkerID
            worker_add if worker_add else "",  # (additional worker specific optional field)
        ]
        self._send_message(message_header, payload)
        return Worker()

    def publish(self, worker_component: WSWorkerComponent, payload: dict):
        header = [
            worker_component.route,
            GInsWSMessageTypes.WSMsgType_Publish.value,
            worker_component.worker_type.value,
            worker_component.id,
            "",
        ]
        logging.info(f"Publishing message: {payload}")
        self._send_message(header, payload)

    def reconfigure(self, worker, config):
        pass

    def destroy_all(self):
        pass

    def pre_defined_worker_map(self):
        return {}

    def worker_map_by_worker_type(self, worker_type):
        return {}

    def authenticate(self, payload):
        pass


class GInsWebSocket:
    """Singleton"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self.ws = None
            self._initialized = True

    def connect(self, url, on_open=None, ssl_context=None):
        self.ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        wst = threading.Thread(
            target=self.ws.run_forever, kwargs={"sslopt": {"context": ssl_context}}
        )
        wst.start()
        # Since the connection is asynchronous, we can't return a SocketService here

    def disconnect(self):
        if self.ws:
            self.ws.close()

    def on_message(self, ws, message):
        global message_counter
        with lock:
            message_counter += 1
        #logging.info("Received message: %s", message)
        pass

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws, arg1, arg2):
        print("WebSocket closed")


class WorkerSubscribeResponsePayload:
    def __init__(self):
        self._id = ""
        self.Id = 0
        self.Success = False


class AdditionalInformation:
    def __init__(self):
        self.messageType = None  # GInsWSMessageTypes
        self.messageFormat = None  # GInsWSWorkerMessageFormat


class SubscriberEvents:
    def on_subscribe(self, id, payload, additional_information):
        pass

    def on_receive(self, id, payload, additional_information):
        logging.info(f"Online data: {payload}")

    def on_error(self, id, payload, additional_information):
        pass

    def on_end(self, id, payload, additional_information):
        pass


class Subscriber:
    def __init__(self, id, events):
        self.id = id
        self.events = events

    def notify(self, message_type, payload, message_format):
        if message_type == GInsWSMessageTypes.WSMsgType_Publish:
            self.events.on_receive(self.id, payload, AdditionalInformation())


class GInsWSClient:
    GInsWSWorkerTypes = GInsWSWorkerTypes
    GInsWebSocket = GInsWebSocket()
    Subscriber = Subscriber


class GIWebSocket:
    def __init__(self, is_ssl):
        self.is_ssl = is_ssl
        self.ws_client = GInsWSClient
        self.workers = {}
        self.uncreated_workers = []
        self.socket_service = None

    def create_url(self):
        if IS_LOCAL_TEST_VERSION:
            return f"{WS_LOCATION}/ws"
        else:
            protocol = "wss://" if self.is_ssl else "ws://"
            url = protocol + WS_LOCATION
            return f"{url}/ws"

    def connect(self, login_required, worker_component: WSWorkerComponent) -> None:
        instance = self.ws_client.GInsWebSocket
        url = self.create_url()
        if login_required:
            access_token = ACCESS_TOKEN
            url += f"?apitoken={access_token}"
        self.uncreated_workers.append(worker_component)

        # We connect
        def on_open(ws):
            self.socket_service = SocketService(ws)
            # Register pre-defined environment worker
            pre_defined_map = self.socket_service.pre_defined_worker_map()
            if pre_defined_map:
                self.create_pre_defined_worker(pre_defined_map)
            for uncreated_worker_component in self.uncreated_workers:
                self.create_worker(uncreated_worker_component)
            self.uncreated_workers = []

            logging.info("Sent formatted initial message to server.")

        # for wss connections
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())
        ssl_context.load_verify_locations(certifi.where())

        instance.connect(
            url, on_open=on_open, ssl_context=ssl_context
        )
        # instance.connect(url, on_open=on_open)

    def create_pre_defined_worker(self, pre_defined_worker):
        if self.socket_service:
            for worker_type, worker_id_arr in pre_defined_worker.items():
                worker_map = self.socket_service.worker_map_by_worker_type(worker_type)
                for worker_id in worker_id_arr:
                    worker = worker_map.get(worker_id)
                    if worker:
                        subscriber = self.ws_client.Subscriber(
                            1, self.default_worker_subscriber_config(WSWorkerComponent())
                        )
                        worker.add_subscriber(subscriber)

    def disconnect(self):
        self.destroy_worker_all()
        instance = self.ws_client.GInsWebSocket
        instance.disconnect()

    def create_worker(self, worker_component: WSWorkerComponent):
        if self.socket_service:
            worker_config = worker_component.config
            route = worker_component.route
            worker = self.socket_service.subscribe(
                worker_component.id, worker_component.worker_type, worker_config, route
            )
            logging.info(
                f"Created worker with ID: {worker_component.id},"
                f" WorkerType: {worker_component.worker_type.value}, "
                f"Route: {route}"
            )
            subscriber = self.ws_client.Subscriber(
                worker_component.id, self.default_worker_subscriber_config(worker_component)
            )
            worker.add_subscriber(subscriber)
            self.workers[worker_component.id] = worker
        else:
            self.uncreated_workers.append(worker_component)

    def destroy_worker_all(self):
        if self.socket_service:
            self.socket_service.destroy_all()

    def authenticate(self, access_token):
        if self.socket_service:
            payload = {"AuthToken": access_token}
            self.socket_service.authenticate(payload)

    def default_worker_subscriber_config(self, component):
        events = SubscriberEvents()

        def on_subscribe(id, payload, additional_information):
            pass

        def on_receive(id, payload, additional_information):
            component.on_receive(payload)

        def on_error(id, payload, additional_information):
            component.on_receive([])

        def on_end(id, payload, additional_information):
            component.on_receive([])

        events.on_subscribe = on_subscribe
        events.on_receive = on_receive
        events.on_error = on_error
        events.on_end = on_end

        return events

    def publish(self, worker_component: WSWorkerComponent, payload: dict) -> None:
        if self.socket_service:
            self.socket_service.publish(worker_component, payload)
        else:
            logging.error(f"Worker {worker_component.id} not found for publishing.")


if __name__ == "__main__":
    gi_websocket = GIWebSocket(is_ssl=False)

    # Read Online Data
    VIDs = ["628e2ed6-21a3-11ee-8bf2-a41cb405973e"]

    def get_online_data_config():
        return {
            "IntervalMs": 1,
            "VIDs": VIDs,
            "ExtendedAnswer": True,
            "OnValueChanged": False,
            "Precision": -1,
        }

    component = WSWorkerComponent(
        id=1, worker_type=GInsWSWorkerTypes.WSWorkerType_OnlineData, config=get_online_data_config()
    )
    gi_websocket.connect(login_required=False, worker_component=component)

    # Write Online Data (Setpoint variables) (has to be input/output and check type)
    write_payload = {
        "Variables": ["628e2ed6-21a3-11ee-8bf2-a41cb405973e"],
        "Values": [5.123],
        "Function": "write",
    }

    import time

    time.sleep(3)
    gi_websocket.publish(worker_component=component, payload=write_payload)

    write_payload = {
        "Variables": ["628e2ed6-21a3-11ee-8bf2-a41cb405973e"],
        "Values": [5.126],
        "Function": "write",
    }
    time.sleep(3)
    gi_websocket.publish(worker_component=component, payload=write_payload)




    # Authenticate/System State
    """
    config = {
        "SID": "0b8635da-c51e-11ec-8398-d43b040eddc2",
        "VIDs": ["4bcbdc16-e621-11ec-8426-d43b040eddc2"],
        "Delta": 26,
        "Points": 0,
        "DataOnly": True,
        "Precision": 1,
    }
    component = WSWorkerComponent(
        id=1, worker_type=GInsWSWorkerTypes.WSWorkerType_Authentication, config=config
    )
    # component.workerType = GInsWSWorkerTypes.WSWorkerType_SystemState

    gi_websocket.connect(login_required=True, worker_component=component)
    """
