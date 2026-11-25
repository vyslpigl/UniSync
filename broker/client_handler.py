
import threading
import logging

from clients.client_manager import get_publisher_topics
from broker.topic_manager import is_valid_topic
from networking.socket_utils import send_msg, recv_msg
from networking.protocol import (
    REGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
    VALID_REGISTRATION_TYPES,
    PUBLISH,
    SUBSCRIBE,
    UNSUBSCRIBE,
    ACK,
    HISTORY,
    STATUS,
    ERROR,
    DISCONNECT,
    create_message,
)


logger = logging.getLogger(__name__)


class ClientHandler(threading.Thread):

    def __init__(self, broker, client_socket, address):
        super().__init__(daemon=True)

        self.broker = broker
        self.sock = client_socket
        self.address = address

        self.client_id = None
        self.role = None
        self.registered = False

        # Prevent concurrent writes from corrupting TCP message framing.
        self.send_lock = threading.Lock()

    def send(self, message):
        """
        Safely sends a message through this client's socket.
        """
        with self.send_lock:
            send_msg(self.sock, message)

    def run(self):
        try:
            self.handle_client()

        except (ConnectionError, ValueError, OSError) as exc:
            logger.warning(
                "Client connection error from %s: %s",
                self.address,
                exc,
            )

        except Exception:
            logger.exception(
                "Unexpected error handling client %s",
                self.address,
            )

        finally:
            self.cleanup()

    def handle_client(self):
        """
        Handles the initial registration and the client's
        connection lifecycle.
        """

        # 1. Wait for the client's first message.
        first_message = recv_msg(self.sock)

        if first_message is None:
            return

        # 2. Validate the registration message.
        if not isinstance(first_message, dict):
            self.send_error("Invalid message format")
            return

        message_type = first_message.get("type")

        if message_type not in VALID_REGISTRATION_TYPES:
            self.send_error("First message must be a registration")
            return

        client_id = first_message.get("client_id")

        if not isinstance(client_id, str) or not client_id.strip():
            self.send_error("Missing or invalid client_id")
            return

        client_id = client_id.strip()

        # 3. Determine client role.
        if message_type == REGISTER_PUBLISHER:
            role = "publisher"

        elif message_type == REGISTER_SUBSCRIBER:
            role = "subscriber"

        else:
            self.send_error("Invalid client role")
            return

        # 4. Register with the broker.
        accepted = self.broker.register(
            client_id,
            role,
            self,
        )

        if not accepted:
            self.send_error("client_id already connected")
            return

        self.client_id = client_id
        self.role = role
        self.registered = True

        # Restore saved subscriptions when a subscriber reconnects.
        if self.role == "subscriber":
            try:
                self.broker.restore_subscriptions(self.client_id)

            except Exception:
                logger.exception("Failed to restore subscriptions")
                self.send_error("Failed to restore subscriptions")
                return

        logger.info(
            "%s connected | ID: %s | Address: %s",
            role.capitalize(),
            client_id,
            self.address,
        )

        # 5. Tell the client registration was successful.
        self.send(
            create_message(
                STATUS,
                status="OK",
                ref=message_type,
            )
        )

        # Deliver messages queued while the subscriber was offline.
        if self.role == "subscriber":
            self.broker.deliver_pending_messages(self)

        # 6. Keep the connection alive.
        while True:

            message = recv_msg(self.sock)

            # Client disconnected unexpectedly.
            if message is None:
                break

            if not isinstance(message, dict):
                self.send_error("Invalid message format")
                continue

            message_type = message.get("type")

            # Normal disconnect.
            if message_type == DISCONNECT:
                break

            # Handle published messages.
            if message_type == PUBLISH:

                # Only publishers are allowed to publish.
                if self.role != "publisher":
                    self.send_error(
                        "Only publishers can publish messages"
                    )
                    continue

                # Extract message fields.
                topic = message.get("topic")
                title = message.get("title")
                content = message.get("content")

                # Check whether the topic is valid.
                if not is_valid_topic(topic):
                    self.send_error("Invalid topic")
                    continue

                # Check whether the publisher is authorized
                # to publish to this topic.
                allowed_topics = get_publisher_topics(self.client_id)

                if topic not in allowed_topics:
                    self.send_error(
                        "You are not authorized to publish to this topic"
                    )
                    continue

                # Let the broker validate and store the message.
                accepted, result = self.broker.publish_message(
                    self.client_id,
                    topic,
                    title,
                    content,
                )

                # Reject invalid messages.
                if not accepted:
                    self.send_error(result)
                    continue

                # Confirm that the message was accepted.
                self.send(
                    create_message(
                        STATUS,
                        status="ACCEPTED",
                        ref=PUBLISH,
                        message_id=result["message_id"],
                    )
                )

                continue

            # Handle subscription requests.
            if message_type == SUBSCRIBE:

                # Only subscribers are allowed to subscribe.
                if self.role != "subscriber":
                    self.send_error(
                        "Only subscribers can subscribe to topics"
                    )
                    continue

                # Extract the requested topic.
                topic = message.get("topic")

                # Ask the broker's TopicManager to subscribe this client.
                accepted = self.broker.topic_manager.subscribe(
                    self.client_id,
                    topic,
                )

                # Reject invalid topics.
                if not accepted:
                    self.send_error("Invalid topic")
                    continue

                try:
                    self.broker.queue_repository.save_subscription(
                        self.client_id,
                        topic,
                    )

                except Exception:
                    logger.exception("Failed to save subscription")

                    self.broker.topic_manager.unsubscribe(
                        self.client_id,
                        topic,
                    )

                    self.send_error("Failed to save subscription")
                    continue

                # Confirm successful subscription.
                self.send(
                    create_message(
                        STATUS,
                        status="SUBSCRIBED",
                        ref=SUBSCRIBE,
                        topic=topic,
                    )
                )

                continue

            # Handle unsubscription requests.
            if message_type == UNSUBSCRIBE:

                # Only subscribers are allowed to unsubscribe.
                if self.role != "subscriber":
                    self.send_error(
                        "Only subscribers can unsubscribe from topics"
                    )
                    continue

                # Extract the requested topic.
                topic = message.get("topic")

                # Ask the broker's TopicManager to unsubscribe this client.
                accepted = self.broker.topic_manager.unsubscribe(
                    self.client_id,
                    topic,
                )

                # Reject invalid topics.
                if not accepted:
                    self.send_error("Invalid topic")
                    continue

                try:
                    self.broker.queue_repository.remove_subscription(
                        self.client_id,
                        topic,
                    )

                except Exception:
                    logger.exception("Failed to remove subscription")

                    self.broker.topic_manager.subscribe(
                        self.client_id,
                        topic,
                    )

                    self.send_error("Failed to remove subscription")
                    continue

                # Confirm successful unsubscription.
                self.send(
                    create_message(
                        STATUS,
                        status="UNSUBSCRIBED",
                        ref=UNSUBSCRIBE,
                        topic=topic,
                    )
                )

                continue

            # Handle message history requests.
            if message_type == HISTORY:

                try:
                    if self.role == "subscriber":
                        messages = (
                            self.broker.queue_repository
                            .get_subscriber_message_history(self.client_id)
                        )

                    elif self.role == "publisher":
                        messages = (
                            self.broker.queue_repository
                            .get_publisher_message_history(self.client_id)
                        )

                    else:
                        self.send_error("Invalid client role")
                        continue

                    self.send(
                        create_message(
                            HISTORY,
                            messages=messages,
                        )
                    )

                except Exception:
                    logger.exception(
                        "Failed to retrieve message history"
                    )
                    self.send_error(
                        "Failed to retrieve message history"
                    )

                continue

            # Handle message acknowledgments.
            if message_type == ACK:

                # Only subscribers can acknowledge messages.
                if self.role != "subscriber":
                    self.send_error(
                        "Only subscribers can acknowledge messages"
                    )
                    continue

                # Validate the message ID.
                message_id = message.get("message_id")

                if not isinstance(message_id, str) or not message_id:
                    self.send_error("Invalid message ID in ACK")
                    continue

                # Remove the pending delivery for this subscriber.
                acknowledged = self.broker.acknowledge_message(
                    self.client_id,
                    message_id,
                )

                if acknowledged:
                    logger.info(
                        "Message acknowledged | Subscriber: %s | Message: %s",
                        self.client_id,
                        message_id,
                    )

                continue

            # Other message types are not supported.
            self.send_error(
                f"Unsupported message type: {message_type}"
            )

    def send_error(self, reason):
        """
        Sends an ERROR response to the client.
        """
        try:
            self.send(
                create_message(
                    ERROR,
                    reason=reason,
                )
            )

        except (ConnectionError, OSError):
            logger.debug(
                "Could not send error to %s",
                self.address,
            )

    def cleanup(self):
        """
        Removes the client from the broker and closes its socket.
        """

        if self.registered:

            if self.role == "subscriber":
                self.broker.topic_manager.remove_subscriber(
                    self.client_id
                )

            self.broker.unregister(self.client_id)

            logger.info(
                "%s disconnected | ID: %s",
                self.role.capitalize(),
                self.client_id,
            )

            self.registered = False

        try:
            self.sock.shutdown(2)

        except OSError:
            pass

        try:
            self.sock.close()

        except OSError:
            pass
