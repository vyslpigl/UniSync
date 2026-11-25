import tempfile
import os

from storage import database
from storage.database import initialize_database
from storage.queue_repository import QueueRepository


def test_message_history_returns_newest_first():
    original_path = database.DATABASE_PATH

    with tempfile.TemporaryDirectory() as temp_dir:
        database.DATABASE_PATH = os.path.join(
            temp_dir, "test_history.db"
        )

        try:
            initialize_database()
            repository = QueueRepository()

            repository.save_message({
                "message_id": "MSG-001",
                "publisher_id": "PUB-001",
                "topic": "announcements",
                "title": "First",
                "content": "First message",
                "timestamp": 100
            })

            repository.save_message({
                "message_id": "MSG-002",
                "publisher_id": "PUB-001",
                "topic": "announcements",
                "title": "Second",
                "content": "Second message",
                "timestamp": 200
            })

            history = repository.get_message_history()

            assert [m["message_id"] for m in history] == [
                "MSG-002", "MSG-001"
            ]

        finally:
            database.DATABASE_PATH = original_path
