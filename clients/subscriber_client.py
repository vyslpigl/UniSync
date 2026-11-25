
import socket
import threading
import queue

from networking.socket_utils import send_msg, recv_msg
from networking.protocol import (
    REGISTER_SUBSCRIBER,
    SUBSCRIBE,
    UNSUBSCRIBE,
    STATUS,
    ERROR,
    MESSAGE,
    ACK,
    HISTORY,
    DISCONNECT,
    create_message,
)


HOST = "127.0.0.1"
PORT = 5000
SEND_LOCK = threading.Lock()

UNIVERSITY_TOPICS = {
    "1": "university.announcements",
    "2": "university.academic",
    "3": "university.events",
    "4": "university.emergency",
    "5": "university.it",
    "6": "university.library",
}

COLLEGE_TOPICS = {
    "7": "college.ict",
    "8": "college.business",
    "9": "college.engineering",
    "10": "college.education",
    "11": "college.arts_sciences",
    "12": "college.nursing",
    "13": "college.agriculture",
}

ALL_TOPICS = {
    **UNIVERSITY_TOPICS,
    **COLLEGE_TOPICS,
}


def display_topics():
    print("\n===== UniSync Available Topics =====")

    for number, topic in ALL_TOPICS.items():
        print(f"{number}. {topic}")

    print("===============================")


def listen_for_messages(sock, response_queue, stop_event):
    """
    Receives all messages from the broker.

    This is the only function that reads from the socket
    after subscriber registration.
    """

    while not stop_event.is_set():

        try:
            response = recv_msg(sock)

            if response is None:
                print("\nBroker connection closed.")
                break

            message_type = response.get("type")

            if message_type == MESSAGE:
                print("\n\n===== NEW NOTIFICATION =====")
                print("Topic:", response.get("topic"))
                print("Title:", response.get("title"))
                print("Content:", response.get("content"))
                print("Message ID:", response.get("message_id"))
                print("============================\n")
                
                message_id = response.get("message_id")

                if isinstance(message_id, str) and message_id:
                    try:
                        with SEND_LOCK:
                            send_msg(
                                sock,
                                create_message(
                                    ACK,
                                    message_id=message_id,
                                ),
                            )
                        print("Notification Acknowledged.")

                    except (ConnectionError, OSError) as exc:
                        print(f"Failed to acknowledge notification {message_id}: {exc}")
            elif message_type in (STATUS, ERROR, HISTORY):
                response_queue.put(response)



            else:
                print("\nReceived unexpected message:", response)

        except (ConnectionError, OSError, ValueError) as exc:
            if not stop_event.is_set():
                print(f"\nListener error: {exc}")
            break

    stop_event.set()


def send_request(sock, response_queue, request):
    """
    Sends a request and waits for its broker response.
    """

    try:
        with SEND_LOCK:   
            send_msg(sock, request)

        response = response_queue.get(timeout=5)

    except queue.Empty:
        print("No response from broker.")
        return None

    except (ConnectionError, OSError) as exc:
        print(f"Connection error: {exc}")
        return None

    return response


def subscribe_to_topic(sock, response_queue, topic):
    response = send_request(
        sock,
        response_queue,
        create_message(SUBSCRIBE, topic=topic),
    )

    if response is None:
        return

    if response.get("type") == STATUS:
        if response.get("status") == "SUBSCRIBED":
            print(f"Successfully subscribed to {topic}.")
        else:
            print("Subscription failed:", response)

    else:
        print("Subscription failed:", response.get("reason"))


def unsubscribe_from_topic(sock, response_queue, topic):
    response = send_request(
        sock,
        response_queue,
        create_message(UNSUBSCRIBE, topic=topic),
    )

    if response is None:
        return

    if response.get("type") == STATUS:
        if response.get("status") == "UNSUBSCRIBED":
            print(f"Successfully unsubscribed from {topic}.")
        else:
            print("Unsubscription failed:", response)

    else:
        print("Unsubscription failed:", response.get("reason"))

def view_message_history(sock, response_queue):
    response = send_request(
        sock,
        response_queue,
        create_message(HISTORY),
    )

    if response is None:
        return

    if response.get("type") == ERROR:
        print("Failed to retrieve history:", response.get("reason"))
        return

    messages = response.get("messages", [])

    print("\n===== Your Received Message History =====")

    if not messages:
        print("You have no received messages yet.")
        return

    for message in messages:
        print("\n------------------------------")
        print("Topic:", message.get("topic"))
        print("Title:", message.get("title"))
        print("Content:", message.get("content"))
        print("Message ID:", message.get("message_id"))

    print("\n==========================================")

    
def select_topic():
    display_topics()

    choice = input("Select a topic number: ").strip()

    topic = ALL_TOPICS.get(choice)

    if topic is None:
        print("Invalid topic selection.")
        return None

    return topic


def subscriber_menu(sock, response_queue):
    while True:
        print("\n===== UniSync Subscriber Menu =====")
        print("1. Subscribe to a topic")
        print("2. Unsubscribe from a topic")
        print("3. View available topics")
        print("4. View received message history")
        print("5. Disconnect")

        choice = input("Select an option: ").strip()

        if choice == "1":
            topic = select_topic()

            if topic:
                subscribe_to_topic(sock, response_queue, topic)

        elif choice == "2":
            topic = select_topic()

            if topic:
                unsubscribe_from_topic(sock, response_queue, topic)

        elif choice == "3":
            display_topics()

        elif choice == "4":
            view_message_history(sock, response_queue)

        elif choice == "5":
            break

        else:
            print("Invalid choice. Please try again.")


def run_subscriber(client_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    stop_event = threading.Event()
    response_queue = queue.Queue()
    listener_thread = None

    try:
        sock.connect((HOST, PORT))

        # Register as a subscriber.
        send_msg(
            sock,
            create_message(
                REGISTER_SUBSCRIBER,
                client_id=client_id,
            ),
        )

        # Read registration response before starting listener.
        response = recv_msg(sock)

        if response is None or response.get("type") != STATUS:
            print("Registration failed:", response)
            return

        print(f"Subscriber '{client_id}' connected to UniSync.")

        # Start the background listener.
        listener_thread = threading.Thread(
            target=listen_for_messages,
            args=(sock, response_queue, stop_event),
            daemon=True,
        )

        listener_thread.start()

        # Open subscriber menu.
        subscriber_menu(sock, response_queue)

        # Notify broker before closing connection.
        with SEND_LOCK:
            send_msg(sock, create_message(DISCONNECT))
        print("Subscriber disconnected.")

    except (ConnectionError, OSError) as exc:
        print(f"Connection error: {exc}")

    finally:
        stop_event.set()

        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        if listener_thread is not None:
            listener_thread.join(timeout=2)

        sock.close()


if __name__ == "__main__":
    print("\n===== UniSync Subscriber Registration =====")

    client_id = input(
        "Enter your Student Number Number: "
    ).strip()

    if client_id:
        run_subscriber(client_id)
    else:
        print("Subscriber ID cannot be empty.")


