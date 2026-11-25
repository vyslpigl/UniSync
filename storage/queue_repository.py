import time

from storage.database import get_connection


class QueueRepository:

    def register_subscriber(self, subscriber_id):
        """Permanently registers a unique subscriber ID."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                INSERT OR IGNORE INTO subscribers
                (subscriber_id, registered_at)
                VALUES (?, ?)
            """, (subscriber_id, time.time()))

            connection.commit()
            return cursor.rowcount > 0

        finally:
            connection.close()


    def subscriber_exists(self, subscriber_id):
        """Checks whether a subscriber ID is already registered."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT 1 FROM subscribers
                WHERE subscriber_id = ?
            """, (subscriber_id,))

            return cursor.fetchone() is not None

        finally:
            connection.close()

    def save_message(self, message):
        """
        Saves a published message to the database.
        """
        connection = get_connection()

        try:
            cursor = connection.cursor()

            cursor.execute("""
                INSERT INTO messages (
                    message_id,
                    publisher_id,
                    topic,
                    title,
                    content,
                    timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message["message_id"],
                message["publisher_id"],
                message["topic"],
                message["title"],
                message["content"],
                message["timestamp"]
            ))

            connection.commit()
            return True

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    def get_message(self, message_id):
        """
        Retrieves a message using its ID.
        """
        connection = get_connection()

        try:
            cursor = connection.cursor()

            cursor.execute(
                "SELECT * FROM messages WHERE message_id = ?",
                (message_id,)
            )

            result = cursor.fetchone()

            return dict(result) if result else None

        finally:
            connection.close()

    def get_messages_by_topic(self, topic):
        """
        Retrieves all messages belonging to a topic.
        """
        connection = get_connection()

        try:
            cursor = connection.cursor()

            cursor.execute("""
                SELECT * FROM messages
                WHERE topic = ?
                ORDER BY timestamp DESC
            """, (topic,))

            results = cursor.fetchall()

            return [dict(row) for row in results]

        finally:
            connection.close()

    def get_message_history(self, limit=100):
        """Retrieves the latest published messages."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT * FROM messages
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))

            return [dict(row) for row in cursor.fetchall()]

        finally:
            connection.close()

    def save_subscription(self, subscriber_id, topic):
        """Saves a subscriber's topic subscription."""
        connection = get_connection()

        try:
            connection.execute("""
                INSERT OR IGNORE INTO subscriptions
                (subscriber_id, topic)
                VALUES (?, ?)
            """, (subscriber_id, topic))

            connection.commit()
            return True

        finally:
            connection.close()

    def remove_subscription(self, subscriber_id, topic):
        """Removes a subscriber's topic subscription."""
        connection = get_connection()

        try:
            connection.execute("""
                DELETE FROM subscriptions
                WHERE subscriber_id = ? AND topic = ?
            """, (subscriber_id, topic))

            connection.commit()
            return True

        finally:
            connection.close()

    def get_subscriptions(self, subscriber_id):
        """Retrieves all topics followed by a subscriber."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT topic FROM subscriptions
                WHERE subscriber_id = ?
                ORDER BY topic
            """, (subscriber_id,))

            return [row["topic"] for row in cursor.fetchall()]

        finally:
            connection.close()

    def get_subscribers_by_topic(self, topic):
        """Retrieves all subscribers of a topic."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT subscriber_id FROM subscriptions
                WHERE topic = ?
            """, (topic,))

            return [row["subscriber_id"] for row in cursor.fetchall()]

        finally:
            connection.close()

    def add_pending_delivery(self, subscriber_id, message_id):
        """Adds a message to a subscriber's pending delivery queue."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                INSERT OR IGNORE INTO pending_deliveries
                (subscriber_id, message_id, queued_at)
                VALUES (?, ?, ?)
            """, (subscriber_id, message_id, time.time()))

            connection.commit()
            return cursor.rowcount > 0

        finally:
            connection.close()

    def record_delivery_attempt(self, subscriber_id, message_id, retry_delay):
        """Records a delivery attempt and schedules the next retry."""
        connection = get_connection()

        try:
            now = time.time()

            cursor = connection.execute("""
                UPDATE pending_deliveries
                SET attempts = attempts + 1,
                    last_attempt = ?,
                    next_retry = ?
                WHERE subscriber_id = ? AND message_id = ?
            """, (
                now,
                now + retry_delay,
                subscriber_id,
                message_id
            ))

            connection.commit()
            return cursor.rowcount > 0

        finally:
            connection.close()

    def has_pending_delivery(self, subscriber_id, message_id):
        """Checks whether a message is pending, regardless of retry time."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT 1
                FROM pending_deliveries
                WHERE subscriber_id = ? AND message_id = ?
                LIMIT 1
            """, (subscriber_id, message_id))

            return cursor.fetchone() is not None

        finally:
            connection.close()

    def get_pending_deliveries(self, subscriber_id):
        """Retrieves pending messages that are ready for delivery."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT messages.*
                FROM pending_deliveries
                JOIN messages
                    ON pending_deliveries.message_id = messages.message_id
                WHERE pending_deliveries.subscriber_id = ?
                    AND (
                        pending_deliveries.next_retry IS NULL
                        OR pending_deliveries.next_retry <= ?
                    )
                ORDER BY pending_deliveries.queued_at ASC
            """, (subscriber_id, time.time()))

            return [dict(row) for row in cursor.fetchall()]

        finally:
            connection.close()

    def remove_pending_delivery(self, subscriber_id, message_id):
        """Removes a pending delivery after receiving a valid ACK."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                DELETE FROM pending_deliveries
                WHERE subscriber_id = ? AND message_id = ?
            """, (subscriber_id, message_id))

            connection.commit()
            return cursor.rowcount > 0

        finally:
            connection.close()

    def record_delivery_history(self, subscriber_id, message_id):
        """Records a message successfully received by a subscriber."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                INSERT OR IGNORE INTO delivery_history
                (subscriber_id, message_id, received_at)
                VALUES (?, ?, ?)
            """, (subscriber_id, message_id, time.time()))

            connection.commit()
            return cursor.rowcount > 0

        finally:
            connection.close()

    def get_subscriber_message_history(self, subscriber_id):
        """Retrieves all messages received by a subscriber."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT messages.*, delivery_history.received_at
                FROM delivery_history
                JOIN messages
                    ON delivery_history.message_id = messages.message_id
                WHERE delivery_history.subscriber_id = ?
                ORDER BY delivery_history.received_at DESC
            """, (subscriber_id,))

            return [dict(row) for row in cursor.fetchall()]

        finally:
            connection.close()

    def get_publisher_message_history(self, publisher_id):
        """Retrieves all messages published by a publisher."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT * FROM messages
                WHERE publisher_id = ?
                ORDER BY timestamp DESC
            """, (publisher_id,))

            return [dict(row) for row in cursor.fetchall()]

        finally:
            connection.close()

    def get_due_deliveries(self):
        """Retrieves pending deliveries that are ready for retry."""
        connection = get_connection()

        try:
            cursor = connection.execute("""
                SELECT
                    pending_deliveries.subscriber_id,
                    messages.*
                FROM pending_deliveries
                JOIN messages
                    ON pending_deliveries.message_id = messages.message_id
                WHERE pending_deliveries.next_retry IS NULL
                    OR pending_deliveries.next_retry <= ?
                ORDER BY pending_deliveries.queued_at ASC
            """, (time.time(),))

            return [dict(row) for row in cursor.fetchall()]

        finally:
            connection.close()

