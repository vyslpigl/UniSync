import socket

from networking.socket_utils import send_msg, recv_msg
from networking.protocol import (
    REGISTER_PUBLISHER,
    PUBLISH,
    STATUS,
    ERROR,
    HISTORY,
    DISCONNECT,
    create_message,
)

from clients.client_manager import (
    PUBLISHER_PROFILES,
    get_publisher_topics,
)

from broker.topic_manager import UNIVERSITY_TOPICS, COLLEGE_TOPICS


HOST = "127.0.0.1"
PORT = 5000

def choose_publisher():
    print("\n=== UniSync Publisher Registration ===")
    print("\nAvailable University Offices:")

    publishers = list(PUBLISHER_PROFILES.items())

    for index, (publisher_id, profile) in enumerate(publishers, start=1):
        print(f"{index}. {profile['name']}")

    print("0. Exit")

    choice = input("\nSelect your office: ").strip()

    if choice == "0":
        return None

    if not choice.isdigit():
        print("Invalid choice.")
        return None

    choice = int(choice)

    if choice < 1 or choice > len(publishers):
        print("Invalid choice.")
        return None

    publisher_id, profile = publishers[choice - 1]

    entered_id = input("\nEnter Publisher ID: ").strip()

    if entered_id != publisher_id:
        print("Invalid Publisher ID. Access denied.")
        return None

    return publisher_id, profile

def choose_topic(publisher_id):
    topics = sorted(get_publisher_topics(publisher_id))

    print("\nAvailable Topics:")

    for index, topic in enumerate(topics, start=1):
        print(f"{index}. {topic}")

    print("0. Cancel")

    choice = input("Choose a topic number: ").strip()

    if choice == "0":
        return None

    if not choice.isdigit():
        print("Invalid choice.")
        return None

    choice = int(choice)

    if choice < 1 or choice > len(topics):
        print("Invalid choice.")
        return None

    return topics[choice - 1]


def compose_message():
    print("\nCompose Announcement")

    title = input("Announcement title: ").strip()
    content = input("Announcement content: ").strip()

    if not title:
        print("Title cannot be empty.")
        return None

    if not content:
        print("Content cannot be empty.")
        return None

    return {
        "title": title,
        "content": content,
    }


def create_announcement(publisher_id):
    topic = choose_topic(publisher_id)

    if topic is None:
        return None

    message = compose_message()

    if message is None:
        return None

    announcement = {
        "topic": topic,
        "title": message["title"],
        "content": message["content"],
    }

    return announcement


def send_announcement(sock, announcement):
    request = create_message(
        PUBLISH,
        topic=announcement["topic"],
        title=announcement["title"],
        content=announcement["content"],
    )

    send_msg(sock, request)

    response = recv_msg(sock)

    if response is None:
        print("No response received from broker.")
        return False

    if response.get("type") == ERROR:
        print("Publishing failed:", response.get("reason"))
        return False

    if (
        response.get("type") == STATUS
        and response.get("status") == "ACCEPTED"
    ):
        print("\nAnnouncement published successfully!")
        print("Message ID:", response.get("message_id"))
        return True

    print("Unexpected broker response:", response)
    return False

def view_message_history(sock):
    send_msg(
        sock,
        create_message(HISTORY),
    )

    response = recv_msg(sock)

    if response is None:
        print("No response received from broker.")
        return

    if response.get("type") == ERROR:
        print("Failed to retrieve history:", response.get("reason"))
        return

    if response.get("type") != HISTORY:
        print("Unexpected broker response:", response)
        return

    messages = response.get("messages", [])

    print("\n===== Your Published Message History =====")

    if not messages:
        print("You have not published any messages yet.")
        return

    for message in messages:
        print("\n------------------------------")
        print("Topic:", message.get("topic"))
        print("Title:", message.get("title"))
        print("Content:", message.get("content"))
        print("Message ID:", message.get("message_id"))

    print("\n==========================================")

def run_publisher(client_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect((HOST, PORT))

        # Register as a publisher.
        send_msg(
            sock,
            create_message(
                REGISTER_PUBLISHER,
                client_id=client_id,
            ),
        )

        response = recv_msg(sock)

        if (
            response is None
            or response.get("type") != STATUS
            or response.get("status") != "OK"
        ):
            print("Registration failed:", response)
            return

        print(f"Publisher '{client_id}' connected to UniSync.")

        print("\nCommands:")
        print("  publish - Create and send an announcement")
        print("  history - View your published messages")
        print("  quit    - Disconnect from UniSync")

        while True:
            command = input("\n> ").strip().lower()

            if command == "quit":
                break

            elif command == "publish":
                announcement = create_announcement(client_id)

                if announcement is None:
                    print("Announcement creation cancelled.")
                    continue

                send_announcement(sock, announcement)

            elif command == "history":
                view_message_history(sock)

            else:
                print("Unknown command. Type 'publish' or 'quit'.")

        # Disconnect from the broker.
        send_msg(
            sock,
            create_message(DISCONNECT),
        )

        print("Publisher disconnected.")

    except (ConnectionError, OSError) as exc:
        print(f"Connection error: {exc}")

    finally:
        sock.close()


if __name__ == "__main__":
    selected_publisher = choose_publisher()

    if selected_publisher is not None:
        publisher_id, profile = selected_publisher

        print(f"\nWelcome, {profile['name']}!")

        run_publisher(publisher_id)