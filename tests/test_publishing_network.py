import unittest
import socket
import os
import tempfile

from broker.broker_server import BrokerServer
from storage import database
from networking.socket_utils import send_msg, recv_msg
from networking.protocol import (
    REGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
    PUBLISH,
    SUBSCRIBE,
    UNSUBSCRIBE,
    STATUS,
    ERROR,
    create_message,
)


class PublishingNetworkTests(unittest.TestCase):

    def setUp(self):
        # Use a temporary database for each network test.
        self.original_database_path = database.DATABASE_PATH
        self.temp_dir = tempfile.TemporaryDirectory()
        database.DATABASE_PATH = os.path.join(
            self.temp_dir.name,
            "test_unisync.db",
        )

        # Keep track of sockets created during each test.
        self.client_sockets = []

        # Start a broker on an automatically assigned port.
        self.broker = BrokerServer(port=0)
        self.broker.start()

    def tearDown(self):
        # Close test client sockets.
        for sock in self.client_sockets:
            try:
                sock.close()
            except OSError:
                pass

        # Stop the broker.
        self.broker.stop()

        # Restore the original database location.
        database.DATABASE_PATH = self.original_database_path
        self.temp_dir.cleanup()

    def connect_client(self, client_id, registration_type):
        # Connect to the running broker.
        sock = socket.create_connection(
            (self.broker.host, self.broker.port),
            timeout=3,
        )

        self.client_sockets.append(sock)

        # Register the client.
        send_msg(
            sock,
            create_message(
                registration_type,
                client_id=client_id,
            ),
        )

        # Receive registration response.
        response = recv_msg(sock)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "OK")

        return sock

    def test_publisher_can_publish_over_tcp(self):
        publisher = self.connect_client(
            "PUB-004",
            REGISTER_PUBLISHER,
        )

        # Send an actual PUBLISH request through TCP.
        send_msg(
            publisher,
            create_message(
                PUBLISH,
                topic="college.ict",
                title="System Maintenance",
                content="The system will undergo maintenance tonight.",
            ),
        )

        # Receive broker acknowledgment.
        response = recv_msg(publisher)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "ACCEPTED")
        self.assertEqual(response["ref"], PUBLISH)
        self.assertIn("message_id", response)

        # Verify the message actually reached broker storage.
        message_id = response["message_id"]

        self.assertIn(message_id, self.broker.messages)

        stored_message = self.broker.messages[message_id]

        self.assertEqual(stored_message["publisher_id"], "PUB-004")
        self.assertEqual(stored_message["topic"], "college.ict")
        self.assertEqual(stored_message["title"], "System Maintenance")
        self.assertEqual(
            stored_message["content"],
            "The system will undergo maintenance tonight.",
        )

    def test_subscriber_cannot_publish_over_tcp(self):
        subscriber = self.connect_client(
            "STU-001",
            REGISTER_SUBSCRIBER,
        )

        # Attempt to publish as a subscriber.
        send_msg(
            subscriber,
            create_message(
                PUBLISH,
                topic="college.ict",
                title="Unauthorized Message",
                content="This should be rejected.",
            ),
        )

        # Receive rejection.
        response = recv_msg(subscriber)

        self.assertEqual(response["type"], ERROR)
        self.assertEqual(
            response["reason"],
            "Only publishers can publish messages",
        )

        # The broker must not store the rejected message.
        self.assertEqual(len(self.broker.messages), 0)

    def test_invalid_publish_requests_do_not_disconnect_publisher(self):
        publisher = self.connect_client(
            "PUB-004",
            REGISTER_PUBLISHER,
        )

        invalid_requests = [
            {
                "topic": "college.fake",
                "title": "Test Announcement",
                "content": "Test content",
                "expected_reason": "Invalid topic",
            },
            {
                "topic": "college.ict",
                "title": "   ",
                "content": "Test content",
                "expected_reason": "Title cannot be empty",
            },
            {
                "topic": "college.ict",
                "title": "Test Announcement",
                "content": "   ",
                "expected_reason": "Content cannot be empty",
            },
        ]

        for request in invalid_requests:
            # Send invalid publishing request.
            send_msg(
                publisher,
                create_message(
                    PUBLISH,
                    topic=request["topic"],
                    title=request["title"],
                    content=request["content"],
                ),
            )

            # Broker should reject the request.
            response = recv_msg(publisher)

            self.assertEqual(response["type"], ERROR)
            self.assertEqual(
                response["reason"],
                request["expected_reason"],
            )

        # The same publisher should still be connected.
        # Send a valid message after the invalid requests.
        send_msg(
            publisher,
            create_message(
                PUBLISH,
                topic="college.ict",
                title="Valid Announcement",
                content="The publisher connection is still working.",
            ),
        )

        response = recv_msg(publisher)

        # The valid request should still be accepted.
        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "ACCEPTED")
        self.assertIn("message_id", response)

        # Only the valid message should have been stored.
        self.assertEqual(len(self.broker.messages), 1)

    def test_subscriber_can_subscribe_over_tcp(self):
        subscriber = self.connect_client(
            "STU-001",
            REGISTER_SUBSCRIBER,
        )

        # Send a SUBSCRIBE request.
        send_msg(
            subscriber,
            create_message(
                SUBSCRIBE,
                topic="college.ict",
            ),
        )

        # Receive broker confirmation.
        response = recv_msg(subscriber)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "SUBSCRIBED")
        self.assertEqual(response["ref"], SUBSCRIBE)
        self.assertEqual(response["topic"], "college.ict")

        # Verify the subscription was stored in TopicManager.
        subscribers = self.broker.topic_manager.get_subscribers(
            "college.ict"
        )

        self.assertIn("STU-001", subscribers)

    def test_subscriber_can_unsubscribe_over_tcp(self):
        subscriber = self.connect_client(
            "STU-002",
            REGISTER_SUBSCRIBER,
        )

        # Subscribe first.
        send_msg(
            subscriber,
            create_message(
                SUBSCRIBE,
                topic="college.ict",
            ),
        )

        subscribe_response = recv_msg(subscriber)

        self.assertEqual(
            subscribe_response["status"],
            "SUBSCRIBED",
        )

        # Now unsubscribe.
        send_msg(
            subscriber,
            create_message(
                UNSUBSCRIBE,
                topic="college.ict",
            ),
        )

        # Receive broker confirmation.
        response = recv_msg(subscriber)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "UNSUBSCRIBED")
        self.assertEqual(response["ref"], UNSUBSCRIBE)
        self.assertEqual(response["topic"], "college.ict")

        # Verify the subscription was removed.
        subscribers = self.broker.topic_manager.get_subscribers(
            "college.ict"
        )

        self.assertNotIn("STU-002", subscribers)

    def test_subscriber_cannot_subscribe_to_invalid_topic(self):
        subscriber = self.connect_client(
            "STU-003",
            REGISTER_SUBSCRIBER,
        )

        # Attempt to subscribe to an invalid topic.
        send_msg(
            subscriber,
            create_message(
                SUBSCRIBE,
                topic="college.fake",
            ),
        )

        # Receive rejection.
        response = recv_msg(subscriber)

        self.assertEqual(response["type"], ERROR)
        self.assertEqual(response["reason"], "Invalid topic")

        # Invalid subscription must not be stored.
        subscribers = self.broker.topic_manager.get_subscribers(
            "college.fake"
        )

        self.assertNotIn("STU-003", subscribers)

        # Verify the subscriber is still connected by sending
        # a valid subscription request afterward.
        send_msg(
            subscriber,
            create_message(
                SUBSCRIBE,
                topic="college.ict",
            ),
        )

        response = recv_msg(subscriber)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "SUBSCRIBED")

    def test_publisher_cannot_subscribe_over_tcp(self):
        publisher = self.connect_client(
            "PUB-004",
            REGISTER_PUBLISHER,
        )

        # Publisher attempts to subscribe.
        send_msg(
            publisher,
            create_message(
                SUBSCRIBE,
                topic="college.ict",
            ),
        )

        # Receive rejection.
        response = recv_msg(publisher)

        self.assertEqual(response["type"], ERROR)
        self.assertEqual(
            response["reason"],
            "Only subscribers can subscribe to topics",
        )

        # Verify publisher was not added to the subscription.
        subscribers = self.broker.topic_manager.get_subscribers(
            "college.ict"
        )

        self.assertNotIn("PUB-004", subscribers)

        # Verify publisher can still publish after rejection.
        send_msg(
            publisher,
            create_message(
                PUBLISH,
                topic="college.ict",
                title="Test Announcement",
                content="Publisher is still operational.",
            ),
        )

        response = recv_msg(publisher)

        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "ACCEPTED")

    def test_unsubscribe_from_unsubscribed_topic_over_tcp(self):
        subscriber = self.connect_client(
            "STU-004",
            REGISTER_SUBSCRIBER,
        )

        # Unsubscribe without subscribing first.
        send_msg(
            subscriber,
            create_message(
                UNSUBSCRIBE,
                topic="college.ict",
            ),
        )

        response = recv_msg(subscriber)

        # Unsubscription should still be successful.
        self.assertEqual(response["type"], STATUS)
        self.assertEqual(response["status"], "UNSUBSCRIBED")
        self.assertEqual(response["ref"], UNSUBSCRIBE)
        self.assertEqual(response["topic"], "college.ict")

        # No subscription should have been created.
        subscribers = self.broker.topic_manager.get_subscribers(
            "college.ict"
        )

        self.assertNotIn("STU-004", subscribers)


if __name__ == "__main__":
    unittest.main()