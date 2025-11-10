import json
import unittest
from unittest.mock import Mock, patch

# Import the classes from the module containing the websocket client
from gi_data.cloudconnect.gi_websocket import (
    GIWebSocket,
    WSWorkerComponent,
    GInsWSWorkerTypes,
    GInsWSMessageTypes,
    SocketService,
)


class TestGIWebSocket(unittest.TestCase):
    @patch("src.cloudconnect.gi_websocket.SocketService")
    @patch("src.cloudconnect.gi_websocket.GInsWebSocket")
    def test_create_worker_calls_socket_service_subscribe(
        self, mock_gins_websocket, mock_socket_service
    ):
        # Arrange
        mock_ws_instance = Mock()
        mock_gins_websocket.return_value = mock_ws_instance

        mock_socket_service_instance = Mock()
        mock_socket_service.return_value = mock_socket_service_instance

        gi_websocket = GIWebSocket(is_ssl=False)
        gi_websocket.socket_service = mock_socket_service_instance

        worker_component = WSWorkerComponent(
            id=2, worker_type=GInsWSWorkerTypes.WSWorkerType_XmlRpcCall, config={}
        )

        # Act
        gi_websocket.create_worker(worker_component)

        # Assert
        mock_socket_service_instance.subscribe.assert_called_once_with(
            worker_component.id,
            worker_component.worker_type,
            worker_component.config,
            worker_component.route,
        )

    @patch("src.cloudconnect.gi_websocket.SocketService")
    @patch("src.cloudconnect.gi_websocket.GInsWebSocket")
    def test_publish_sends_message_via_socket_service(
        self, mock_gins_websocket, mock_socket_service
    ):
        # Arrange
        mock_ws_instance = Mock()
        mock_gins_websocket.return_value = mock_ws_instance

        mock_socket_service_instance = Mock()
        mock_socket_service.return_value = mock_socket_service_instance

        gi_websocket = GIWebSocket(is_ssl=False)
        gi_websocket.socket_service = mock_socket_service_instance

        worker_component = WSWorkerComponent(
            id=3, worker_type=GInsWSWorkerTypes.WSWorkerType_Authentication, config={}
        )
        payload = {"action": "login", "credentials": "test"}

        # Act
        gi_websocket.publish(worker_component, payload)

        # Assert
        mock_socket_service_instance.publish.assert_called_once_with(worker_component, payload)

    @patch("src.cloudconnect.gi_websocket.GIWebSocket")
    def test_socket_service_send_message_constructs_correctly(self, mock_websocket_app):
        # Arrange
        mock_ws = Mock()
        socket_service = SocketService(mock_ws)
        header = [
            "test_route",
            GInsWSMessageTypes.WSMsgType_Publish.value,
            GInsWSWorkerTypes.WSWorkerType_OnlineData.value,
            4,
            "",
        ]
        payload = {"data": "test_data"}

        # Act
        socket_service._send_message(header, payload)

        # Assert
        mock_ws.send.assert_called_once()
        sent_message = mock_ws.send.call_args[0][0]
        # Since sent_message is binary, we can check parts of it
        self.assertTrue(isinstance(sent_message, bytes))

    @patch("src.cloudconnect.gi_websocket.SocketService")
    @patch("src.cloudconnect.gi_websocket.GInsWebSocket")
    def test_disconnect_closes_websocket(self, mock_gins_websocket, mock_socket_service):
        # Arrange
        mock_ws_instance = Mock()
        mock_gins_websocket.return_value = mock_ws_instance
        gi_websocket = GIWebSocket(is_ssl=False)
        gi_websocket.ws_client.GInsWebSocket = mock_ws_instance

        # Act
        gi_websocket.disconnect()

        # Assert
        mock_ws_instance.disconnect.assert_called_once()

    def test_send_message_header_length(self):
        # Arrange
        mock_ws = Mock()
        socket_service = SocketService(mock_ws)
        header = [
            "test_route",
            GInsWSMessageTypes.WSMsgType_Publish.value,
            GInsWSWorkerTypes.WSWorkerType_OnlineData.value,
            4,
            "",
        ]
        payload = {"data": "test_data"}

        # Act
        socket_service._send_message(header, payload)

        # Assert
        mock_ws.send.assert_called_once()
        sent_message = mock_ws.send.call_args[0][0]

        # Extract version byte, header length, and message body from original sent message (binary)
        version_byte = sent_message[0:1]
        header_len_bytes = sent_message[1:3]
        header_len = int.from_bytes(header_len_bytes, "little")
        message_body_bytes = sent_message[3:]

        # Decode the message body
        message_body = message_body_bytes.decode("utf-8")

        # Extract the header JSON and payload JSON based on header length
        header_json_bytes = message_body_bytes[:header_len]
        payload_json_bytes = message_body_bytes[header_len:]

        header_json = header_json_bytes.decode("utf-8")
        payload_json = payload_json_bytes.decode("utf-8")

        expected_header_json = json.dumps(header, separators=(",", ":"))
        expected_payload_json = json.dumps(payload)

        expected_header_len = len(expected_header_json.encode("utf-8"))

        # Verify that the header length in the message matches the actual length of the header JSON
        self.assertEqual(
            header_len,
            expected_header_len,
            "Header length in message does not match actual header length",
        )

        self.assertEqual(
            header_json,
            expected_header_json,
            "Header JSON in message does not match expected header JSON",
        )

        self.assertEqual(
            payload_json,
            expected_payload_json,
            "Payload JSON in message does not match expected payload JSON",
        )
        self.assertEqual(version_byte, b"\x00", "Version byte in message is incorrect")


if __name__ == "__main__":
    unittest.main()
