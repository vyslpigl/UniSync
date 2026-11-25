import socket
import threading
import logging
import uuid
import time

from broker.topic_manager import TopicManager, is_valid_topic
from clients.client_manager import get_publisher
from broker.client_handler import ClientHandler
from networking.protocol import MESSAGE, create_message
from storage.database import initialize_database
from storage.queue_repository import QueueRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BrokerServer:

    def __init__(self, host="127.0.0.1", port=5000):

        self.host = host
        self.port = port

        self.server_socket = None
        self.accept_thread = None

        self.running = False

        # client_id -> ClientHandler
        self.clients = {}

        # Protects the shared client registry.
        self.lock = threading.Lock()

        # Stores accepted messages in memory.
        self.messages = {}

        # Protects the shared message collection.
        self.message_lock = threading.Lock()

        # Manages topic subscriptions.
        self.topic_manager = TopicManager()
        
        # Initialize SQLite database.
        initialize_database()

        # Handles persistent message storage.
        self.queue_repository = QueueRepository()

        # Controls automatic message delivery retries.
        self.retry_interval = 2
        self.retry_delay = 5
        self.retry_thread = None
   
    def start(self):
        """
        Starts the broker in a background thread.
        """

        if self.running:
            return

        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )

        self.server_socket.bind(
            (self.host, self.port)
        )

        self.server_socket.listen()

        self.running = True

        # Update port when port=0 was used for testing.
        self.port = self.server_socket.getsockname()[1]

        self.accept_thread = threading.Thread(
            target=self.accept_clients,
            daemon=True,
        )

        self.accept_thread.start()

        logger.info(
            "Broker started on %s:%s",
            self.host,
            self.port,
        )

        self.retry_thread = threading.Thread(
            target=self.retry_pending_deliveries,
            daemon=True,
        )
        self.retry_thread.start()

    def retry_pending_deliveries(self):
        """Periodically retries pending message deliveries."""

        while self.running:
            try:
                due_deliveries = (
                    self.queue_repository.get_due_deliveries()
                )

                for message in due_deliveries:
                    subscriber_id = message["subscriber_id"]

                    with self.lock:
                        handler = self.clients.get(subscriber_id)

                        if handler is not None and handler.role != "subscriber":
                            handler = None

                    if handler is None:
                        continue

                    outgoing_message = create_message(
                        MESSAGE,
                        **{
                            key: value
                            for key, value in message.items()
                            if key != "subscriber_id"
                        }
                    )

                    try:
                        handler.send(outgoing_message)

                        self.queue_repository.record_delivery_attempt(
                            subscriber_id,
                            message["message_id"],
                            self.retry_delay,
                        )

                        logger.info(
                            "Retrying message %s for %s",
                            message["message_id"],
                            subscriber_id,
                        )

                    except (ConnectionError, OSError):
                        self.queue_repository.record_delivery_attempt(
                            subscriber_id,
                            message["message_id"],
                            self.retry_delay,
                        )

                        logger.warning(
                            "Retry failed for %s",
                            subscriber_id,
                        )

            except Exception:
                logger.exception("Retry worker encountered an error")

            time.sleep(self.retry_interval)

    def accept_clients(self):
        """
        Accepts incoming connections continuously.

        Each connection gets its own ClientHandler thread.
        """

        while self.running:

            try:
                client_socket, address = self.server_socket.accept()

            except OSError:

                if not self.running:
                    break

                logger.exception("Accept failed")
                continue

            handler = ClientHandler(
                self,
                client_socket,
                address,
            )

            handler.start()




    def register(self, client_id, role, handler):
        if role == "publisher" and get_publisher(client_id) is None:
            return False

        with self.lock:
            if not self.running:
                return False

            # Prevent simultaneous connections using the same ID.
            if client_id in self.clients:
                return False

            if role == "subscriber":
                try:
                    self.queue_repository.register_subscriber(client_id)
                except Exception:
                    logger.exception(
                        "Failed to register subscriber %s",
                        client_id,
                    )
                    return False

            self.clients[client_id] = handler
            return True

    def restore_subscriptions(self, subscriber_id):
        """
        Restores a subscriber's saved topics from SQLite
        into the in-memory TopicManager.
        """

        topics = self.queue_repository.get_subscriptions(
            subscriber_id
        )

        for topic in topics:
            self.topic_manager.subscribe(
                subscriber_id,
                topic,
            )

        logger.info(
            "Restored %d subscriptions for %s",
            len(topics),
            subscriber_id,
        )

    def acknowledge_message(self, subscriber_id, message_id):
        """
        Records successful delivery and removes the pending message.
        """

        try:

            is_pending = self.queue_repository.has_pending_delivery(
                subscriber_id,
                message_id,
            )

            if not is_pending:
                return False

            self.queue_repository.record_delivery_history(
                subscriber_id,
                message_id,
            )

            return self.queue_repository.remove_pending_delivery(
                subscriber_id,
                message_id,
            )

        except Exception:
            logger.exception(
                "Failed to acknowledge message %s for %s",
                message_id,
                subscriber_id,
            )
            return False

        
    def unregister(self, client_id):
        """
        Removes a client safely.
        """

        if client_id is None:
            return

        with self.lock:
            self.clients.pop(client_id, None)

    def get_connected(self):
        """
        Returns a snapshot of connected clients.

        The lock protects the copy from concurrent modifications.
        """

        with self.lock:
            return {
                client_id: handler.role
                for client_id, handler in self.clients.items()
            }

    def stop(self):
        """
        Stops the broker and closes all connected client sockets.
        """

        if not self.running:
            return

        logger.info("Stopping broker...")

        self.running = False

        # Close listening socket to unblock accept().
        try:
            self.server_socket.close()

        except OSError:
            pass

        # Take a snapshot without holding the lock while closing sockets.
        with self.lock:
            handlers = list(self.clients.values())

        # Shutdown client connections.
        # ClientHandler will handle closing its own socket.
        for handler in handlers:

            try:
                handler.sock.shutdown(socket.SHUT_RDWR)

            except OSError:
                pass

        # Wait for client handlers to finish cleanup.
        for handler in handlers:
            handler.join(timeout=2)

        # Wait for the accept loop to finish.
        if self.accept_thread is not None:
            self.accept_thread.join(timeout=2)

        if self.retry_thread is not None:
            self.retry_thread.join(timeout=2)

        logger.info("Broker stopped")


    
    def publish_message(self, publisher_id, topic, title, content):
        """
        Validates, creates, and stores a published message.

        Returns:
            (True, message) if successful
            (False, error_reason) if rejected
        """

        # Validate topic.
        if not is_valid_topic(topic):
            return False, "Invalid topic"

        # Validate title.
        if not isinstance(title, str) or not title.strip():
            return False, "Title cannot be empty"

        # Validate content.
        if not isinstance(content, str) or not content.strip():
            return False, "Content cannot be empty"

        # Generate a unique message ID.
        message_id = f"MSG-{uuid.uuid4().hex}"

        # Create the message record.
        message = {
            "message_id": message_id,
            "publisher_id": publisher_id,
            "topic": topic,
            "title": title.strip(),
            "content": content.strip(),
            "timestamp": time.time(),
        }

        # Save the message permanently in SQLite.
        try:
            self.queue_repository.save_message(message)

        except Exception:
            logger.exception(
                "Failed to save message %s to database",
                message_id,
            )
            return False, "Failed to save message"

        # Store the message in memory.
        with self.message_lock:
            self.messages[message_id] = message

        # Deliver the message to matching subscribers.
        self.route_message(message)

        logger.info(
            "Message published: %s | Topic: %s | Publisher: %s",
            message_id,
            topic,
            publisher_id,
        )

        return True, message
   
    def route_message(self, message):
        """
        Queues messages for all subscribers and sends them
        immediately to connected subscribers.
        """

        # Get all subscribers, including those currently offline.
        subscriber_ids = self.queue_repository.get_subscribers_by_topic(
            message["topic"]
        )

        outgoing_message = create_message(
            MESSAGE,
            **message
        )

        for subscriber_id in subscriber_ids:

            # Save the message in the subscriber's pending queue.
            try:
                self.queue_repository.add_pending_delivery(
                    subscriber_id,
                    message["message_id"],
                )

            except Exception:
                logger.exception(
                    "Failed to queue message for %s",
                    subscriber_id,
                )
                continue

            # Check if the subscriber is connected.
            with self.lock:
                handler = self.clients.get(subscriber_id)

                if handler is not None and handler.role != "subscriber":
                    handler = None

            # Keep the message queued if the subscriber is offline.
            if handler is None:
                continue

            try:
                handler.send(outgoing_message)

                self.queue_repository.record_delivery_attempt(
                    subscriber_id,
                    message["message_id"],
                    self.retry_delay,
                )

                logger.info(
                    "Message %s delivered to %s",
                    message["message_id"],
                    subscriber_id,
                )

            except (ConnectionError, OSError):
                logger.warning(
                    "Could not deliver message to %s",
                    subscriber_id,
                )

            except Exception:
                logger.exception(
                    "Delivery processing failed for %s",
                    subscriber_id,
                )

    def deliver_pending_messages(self, handler):
        """
        Sends queued messages to a subscriber after reconnection.
        """

        pending_messages = self.queue_repository.get_pending_deliveries(
            handler.client_id
        )

        for message in pending_messages:

            outgoing_message = create_message(
                MESSAGE,
                **message
            )

            try:
                handler.send(outgoing_message)

                self.queue_repository.record_delivery_attempt(
                    handler.client_id,
                    message["message_id"],
                    self.retry_delay,
                ) 

            except (ConnectionError, OSError):
                logger.warning(
                    "Failed to send pending message %s to %s",
                    message["message_id"],
                    handler.client_id,
                )
                break

            logger.info(
                "Pending message %s delivered to %s",
                message["message_id"],
                handler.client_id,
            )
if __name__ == "__main__":
    broker = BrokerServer()

    try:
        broker.start()
        print("Press CTRL+C to quit.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        broker.stop()