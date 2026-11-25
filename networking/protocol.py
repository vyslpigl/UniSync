
import time


# Client registration
REGISTER_PUBLISHER = "REGISTER_PUBLISHER"
REGISTER_SUBSCRIBER = "REGISTER_SUBSCRIBER"

PUBLISH = "PUBLISH"
SUBSCRIBE = "SUBSCRIBE"
UNSUBSCRIBE = "UNSUBSCRIBE"

# Broker responses
STATUS = "STATUS"
ERROR = "ERROR"

MESSAGE = "MESSAGE"
ACK = "ACK"
HISTORY = "HISTORY"

# Client lifecycle
DISCONNECT = "DISCONNECT"

# Future message types - add when their phases begin
# SUBSCRIBE = "SUBSCRIBE"
# UNSUBSCRIBE = "UNSUBSCRIBE"
# PUBLISH = "PUBLISH"


VALID_REGISTRATION_TYPES = {
    REGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
}


def create_message(message_type, **fields):
    """
    Creates a standard UniSync message.

    Every message contains:
        type
        timestamp

    Additional fields are added through **fields.
    """

    return {
        "type": message_type,
        "timestamp": time.time(),
        **fields,
    }
