
from storage.database import initialize_database
from storage.queue_repository import QueueRepository

initialize_database()

repository = QueueRepository()

message = {
    "message_id": "MSG-PENDING-001",
    "publisher_id": "PUB-TEST",
    "topic": "university.announcements",
    "title": "Offline Delivery Test",
    "content": "Testing pending message delivery.",
    "timestamp": 1795290185.0
}

# Save the message.
repository.save_message(message)

# Add it to the subscriber's pending queue.
repository.add_pending_delivery(
    "SUB-TEST",
    "MSG-PENDING-001"
)

# Retrieve pending messages.
pending = repository.get_pending_deliveries("SUB-TEST")

print("Pending deliveries:")
print(pending)

# Simulate successful delivery and remove it.
repository.remove_pending_delivery(
    "SUB-TEST",
    "MSG-PENDING-001"
)

print("\nAfter removal:")
print(repository.get_pending_deliveries("SUB-TEST"))
