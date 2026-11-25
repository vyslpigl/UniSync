
import sqlite3
import os
import time

DATABASE_PATH = os.path.join(
    os.path.dirname(__file__),
    "unisync.db"
)


def get_connection():
    """
    Creates and returns a connection to the UniSync database.
    """
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database():
    """
    Creates the required database tables if they do not exist.
    """
    connection = get_connection()

    try:
        cursor = connection.cursor()

        # Stores published messages.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                publisher_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp REAL NOT NULL
            )
        """)

        # Stores persistent subscriber topic subscriptions.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                subscriber_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                PRIMARY KEY (subscriber_id, topic)
            )
        """)

        # Stores permanently registered subscriber IDs.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                subscriber_id TEXT PRIMARY KEY,
                registered_at REAL NOT NULL
            )
        """)

        # Stores messages waiting to be delivered.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_deliveries (
                subscriber_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                queued_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt REAL,
                next_retry REAL,
                PRIMARY KEY (subscriber_id, message_id),
                FOREIGN KEY (message_id)
                    REFERENCES messages(message_id)
                    ON DELETE CASCADE
            )
        """)
                # Stores messages successfully received by each subscriber.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS delivery_history (
                subscriber_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                received_at REAL NOT NULL,
                PRIMARY KEY (subscriber_id, message_id),
                FOREIGN KEY (message_id)
                    REFERENCES messages(message_id)
                    ON DELETE CASCADE
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_subscriber
            ON delivery_history (subscriber_id, received_at)
        """)

                # Preserve subscriber IDs already present in existing records.
        cursor.execute("""
            INSERT OR IGNORE INTO subscribers (subscriber_id, registered_at)
            SELECT subscriber_id, ?
            FROM (
                SELECT subscriber_id FROM subscriptions
                UNION
                SELECT subscriber_id FROM pending_deliveries
            )
        """, (time.time(),))

        # Add retry tracking columns to existing databases.
        existing_columns = {
            row["name"]
            for row in cursor.execute(
                "PRAGMA table_info(pending_deliveries)"
            ).fetchall()
        }

        if "attempts" not in existing_columns:
            cursor.execute("""
                ALTER TABLE pending_deliveries
                ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0
            """)

        if "last_attempt" not in existing_columns:
            cursor.execute("""
                ALTER TABLE pending_deliveries
                ADD COLUMN last_attempt REAL
            """)

        if "next_retry" not in existing_columns:
            cursor.execute("""
                ALTER TABLE pending_deliveries
                ADD COLUMN next_retry REAL
            """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending_subscriber
            ON pending_deliveries (subscriber_id)
        """)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    initialize_database()
    print("UniSync database initialized successfully.")
