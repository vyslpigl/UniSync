
import socket
import customtkinter as ctk
import threading
import queue

from tkinter import messagebox

from clients.client_manager import (
    PUBLISHER_PROFILES,
    get_publisher_topics,
)

from networking.socket_utils import send_msg, recv_msg

from networking.protocol import (
    REGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
    PUBLISH,
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


COLORS = {
    "background": "#F3EFE6",
    "navy": "#142D3A",
    "navy_light": "#1C3946",
    "sidebar": "#123F48",
    "teal": "#176B73",
    "teal_hover": "#125860",
    "white": "#FFFFFF",
    "muted": "#78868A",
    "green": "#68B84A",
    "red": "#C66C62",
    "line": "#D8D4CA",
    "log": "#102B35",
}

ALL_TOPICS = {
    "1": "university.announcements",
    "2": "university.academic",
    "3": "university.events",
    "4": "university.emergency",
    "5": "university.it",
    "6": "university.library",
    "7": "college.ict",
    "8": "college.business",
    "9": "college.engineering",
    "10": "college.education",
    "11": "college.arts_sciences",
    "12": "college.nursing",
    "13": "college.agriculture",
}

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")


class UniSyncApp(ctk.CTk):

    def __init__(self):
        super().__init__()

        self.title("UniSync")
        self.geometry("1000x620")
        self.minsize(850, 560)
        self.configure(fg_color=COLORS["background"])

        self.last_status = None

        # Publisher variables
        self.publisher_socket = None
        self.publisher_id = None
        self.publisher_window = None

        # Subscriber variables
        self.subscriber_socket = None
        self.subscriber_id = None
        self.subscriber_window = None

        self.subscriber_response_queue = queue.Queue()
        self.subscriber_ui_queue = queue.Queue()

        self.subscriber_stop_event = None
        self.subscriber_listener_thread = None

        self.subscriber_send_lock = threading.Lock()
        self.subscriber_request_lock = threading.Lock()

        self.create_layout()
        self.add_log("INFO", "UniSync client started.")
        self.check_broker()

    def create_layout(self):

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # LEFT SIDEBAR
        sidebar = ctk.CTkFrame(
            self,
            width=245,
            corner_radius=0,
            fg_color=COLORS["sidebar"]
        )
        sidebar.grid(row=0, column=0, sticky="nsew")
        sidebar.grid_propagate(False)

        ctk.CTkLabel(
            sidebar,
            text="✉",
            font=("Helvetica", 65),
            text_color=COLORS["background"]
        ).pack(pady=(65, 5))

        ctk.CTkLabel(
            sidebar,
            text="UniSync",
            font=("Menlo", 29, "bold"),
            text_color=COLORS["background"]
        ).pack()

        ctk.CTkLabel(
            sidebar,
            text="WVSU Messaging\nMiddleware",
            font=("Helvetica", 14),
            text_color=COLORS["background"],
            justify="center"
        ).pack(pady=(12, 22))

        ctk.CTkFrame(
            sidebar,
            width=65,
            height=2,
            fg_color="#39777C"
        ).pack(pady=5)

        ctk.CTkLabel(
            sidebar,
            text="UNIVERSITY\nCOMMUNICATION\nMADE SIMPLER",
            font=("Menlo", 11, "bold"),
            text_color="#B8D0CC",
            justify="center"
        ).pack(pady=35)

        ctk.CTkLabel(
            sidebar,
            text="WVSU\nConnecting\nOur Community",
            font=("Helvetica", 13, "italic"),
            text_color="#D0DDDA",
            justify="left"
        ).pack(side="bottom", anchor="w", padx=25, pady=25)

        # RIGHT CONTENT
        content = ctk.CTkFrame(
            self,
            fg_color=COLORS["background"],
            corner_radius=0
        )
        content.grid(row=0, column=1, sticky="nsew", padx=0, pady=0)
        content.grid_columnconfigure(0, weight=1)

        # HEADER
        header = ctk.CTkFrame(
            content,
            fg_color="transparent"
        )
        header.pack(fill="x", padx=28, pady=(20, 5))

        ctk.CTkLabel(
            header,
            text="UNIVERSITY MESSAGING SYSTEM",
            font=("Menlo", 10, "bold"),
            text_color=COLORS["teal"]
        ).pack(side="left")

        self.connection_label = ctk.CTkLabel(
            header,
            text="● Checking broker...",
            font=("Menlo", 11, "bold"),
            text_color=COLORS["muted"]
        )
        self.connection_label.pack(side="right")

        # WELCOME SECTION
        welcome = ctk.CTkFrame(
            content,
            fg_color="transparent"
        )
        welcome.pack(fill="x", padx=25, pady=(30, 12))

        ctk.CTkLabel(
            welcome,
            text="Welcome to UniSync!",
            font=("Menlo", 31, "bold"),
            text_color=COLORS["navy"]
        ).pack(pady=(5, 8))

        ctk.CTkLabel(
            welcome,
            text="Stay connected. Get the latest updates.",
            font=("Helvetica", 15),
            text_color=COLORS["navy"]
        ).pack()

        ctk.CTkFrame(
            content,
            height=2,
            fg_color=COLORS["navy"]
        ).pack(fill="x", padx=30, pady=(12, 15))

        # ROLE CARDS
        cards = ctk.CTkFrame(
            content,
            fg_color="transparent"
        )
        cards.pack(fill="x", padx=35, pady=8)

        cards.grid_columnconfigure((0, 1), weight=1)

        self.create_role_card(
            cards,
            0,
            "📢",
            "Publisher",
            "Publish announcements\nand university updates.",
            "Open Publisher",
            self.open_publisher
        )

        self.create_role_card(
            cards,
            1,
            "👥",
            "Subscriber",
            "Receive updates and manage\nyour subscribed topics.",
            "Open Subscriber",
            self.open_subscriber
        )

        # SYSTEM LOG
        log_panel = ctk.CTkFrame(
            content,
            fg_color=COLORS["log"],
            corner_radius=9,
            height=105
        )
        log_panel.pack(
            side="bottom",
            fill="x",
            padx=18,
            pady=(10, 18)
        )
        log_panel.pack_propagate(False)

        ctk.CTkLabel(
            log_panel,
            text="UNI SYNC  /  SYSTEM LOG",
            font=("Menlo", 9, "bold"),
            text_color="#75B8B0"
        ).pack(anchor="w", padx=14, pady=(9, 2))

        self.log_text = ctk.CTkTextbox(
            log_panel,
            fg_color="transparent",
            text_color="#E6EDE9",
            font=("Menlo", 11),
            border_width=0,
            wrap="word"
        )
        self.log_text.pack(fill="both", expand=True, padx=8, pady=(0, 7))
        self.log_text.configure(state="disabled")

    def create_role_card(
        self, parent, column, icon, title,
        description, button_text, command
    ):

        card = ctk.CTkFrame(
            parent,
            fg_color=COLORS["white"],
            corner_radius=12,
            border_width=1,
            border_color=COLORS["line"],
            height=185
        )
        card.grid(
            row=0,
            column=column,
            padx=9,
            pady=8,
            sticky="nsew"
        )
        card.grid_propagate(False)

        ctk.CTkLabel(
            card,
            text=icon,
            font=("Helvetica", 27),
            text_color=COLORS["navy"]
        ).pack(anchor="w", padx=20, pady=(15, 0))

        ctk.CTkLabel(
            card,
            text=title,
            font=("Helvetica", 22, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", padx=20, pady=(2, 0))

        ctk.CTkLabel(
            card,
            text=description,
            font=("Helvetica", 12),
            text_color=COLORS["muted"],
            justify="left"
        ).pack(anchor="w", padx=20, pady=(5, 8))

        ctk.CTkButton(
            card,
            text=button_text,
            height=36,
            corner_radius=7,
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"],
            font=("Helvetica", 12, "bold"),
            command=command
        ).pack(side="bottom", fill="x", padx=18, pady=14)

    def add_log(self, level, message):

        self.log_text.configure(state="normal")

        self.log_text.insert(
            "end",
            f"[{level}] {message}\n"
        )

        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def check_broker(self):

        try:
            with socket.create_connection(
                ("127.0.0.1", 5000),
                timeout=1
            ):
                connected = True

        except OSError:
            connected = False

        if connected:
            self.connection_label.configure(
                text="● Broker Connected",
                text_color=COLORS["green"]
            )

            if self.last_status is not True:
                self.add_log(
                    "INFO",
                    "Connected to broker at 127.0.0.1:5000"
                )

        else:
            self.connection_label.configure(
                text="● Broker Offline",
                text_color=COLORS["red"]
            )

            if self.last_status is not False:
                self.add_log(
                    "INFO",
                    "Unable to connect to broker."
                )

        self.last_status = connected
        self.after(3000, self.check_broker)

    def open_role(self, role):

        dialog = ctk.CTkToplevel(self)
        dialog.title(f"UniSync - {role}")
        dialog.geometry("450x260")
        dialog.configure(fg_color=COLORS["background"])
        dialog.transient(self)
        dialog.grab_set()

        ctk.CTkLabel(
            dialog,
            text=role,
            font=("Menlo", 25, "bold"),
            text_color=COLORS["navy"]
        ).pack(pady=(55, 15))

        ctk.CTkLabel(
            dialog,
            text="This interface will be connected to\n"
                 "the existing UniSync backend.",
            font=("Helvetica", 13),
            text_color=COLORS["muted"],
            justify="center"
        ).pack(pady=5)

        ctk.CTkButton(
            dialog,
            text="Back",
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"],
            command=dialog.destroy
        ).pack(pady=25)

    # ==========================================
    # PUBLISHER WINDOW
    # ==========================================

    def open_publisher(self):

        if self.publisher_window is not None:
            if self.publisher_window.winfo_exists():
                self.publisher_window.focus()
                return

        self.publisher_window = ctk.CTkToplevel(self)
        window = self.publisher_window

        window.title("UniSync - Publisher")
        window.geometry("750x620")
        window.minsize(650, 550)
        window.configure(fg_color=COLORS["background"])
        window.transient(self)

        window.protocol(
            "WM_DELETE_WINDOW",
            self.close_publisher_window
        )

        # HEADER
        header = ctk.CTkFrame(
            window,
            fg_color=COLORS["sidebar"],
            corner_radius=0,
            height=90
        )
        header.pack(fill="x")
        header.pack_propagate(False)

        ctk.CTkLabel(
            header,
            text="📢  PUBLISHER",
            font=("Menlo", 23, "bold"),
            text_color=COLORS["background"]
        ).pack(anchor="w", padx=25, pady=(18, 2))

        ctk.CTkLabel(
            header,
            text="Publish university announcements and updates",
            font=("Helvetica", 12),
            text_color="#D0DDDA"
        ).pack(anchor="w", padx=27)

        # MAIN CONTENT
        body = ctk.CTkScrollableFrame(
            window,
            fg_color=COLORS["background"]
        )
        body.pack(fill="both", expand=True, padx=22, pady=18)

        # OFFICE SELECTION
        ctk.CTkLabel(
            body,
            text="Publisher Registration",
            font=("Helvetica", 18, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", pady=(0, 12))

        ctk.CTkLabel(
            body,
            text="Select your university office",
            font=("Helvetica", 12),
            text_color=COLORS["muted"]
        ).pack(anchor="w")

        self.publisher_office = ctk.CTkComboBox(
            body,
            values=[
                profile["name"]
                for profile in PUBLISHER_PROFILES.values()
            ],
            width=400,
            height=38
        )
        self.publisher_office.pack(anchor="w", pady=(5, 12))
        self.publisher_office.set("WVSU Administration")

        ctk.CTkLabel(
            body,
            text="Publisher ID",
            font=("Helvetica", 12),
            text_color=COLORS["muted"]
        ).pack(anchor="w")

        self.publisher_id_entry = ctk.CTkEntry(
            body,
            width=400,
            height=38,
            placeholder_text="Enter your Publisher ID"
        )
        self.publisher_id_entry.pack(anchor="w", pady=(5, 12))

        # PUBLISHER CONNECTION BUTTONS
        publisher_connection_buttons = ctk.CTkFrame(
            body,
            fg_color="transparent"
        )
        publisher_connection_buttons.pack(
            anchor="w",
            pady=(0, 15)
        )

        self.publisher_connect_button = ctk.CTkButton(
            publisher_connection_buttons,
            text="Connect to UniSync",
            width=200,
            height=38,
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"],
            command=self.connect_publisher
        )
        self.publisher_connect_button.pack(
            side="left",
            padx=(0, 10)
        )

        self.publisher_disconnect_button = ctk.CTkButton(
            publisher_connection_buttons,
            text="Disconnect",
            width=120,
            height=38,
            fg_color=COLORS["red"],
            hover_color="#A9534B",
            command=self.disconnect_publisher,
            state="disabled"
        )
        self.publisher_disconnect_button.pack(side="left")

        self.publisher_status = ctk.CTkLabel(
            body,
            text="● Not connected",
            font=("Menlo", 11, "bold"),
            text_color=COLORS["red"]
        )
        self.publisher_status.pack(anchor="w", pady=(0, 18))

        # ANNOUNCEMENT SECTION
        ctk.CTkFrame(
            body,
            height=2,
            fg_color=COLORS["line"]
        ).pack(fill="x", pady=8)

        ctk.CTkLabel(
            body,
            text="Create Announcement",
            font=("Helvetica", 18, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", pady=(8, 12))

        ctk.CTkLabel(
            body,
            text="Topic",
            font=("Helvetica", 12),
            text_color=COLORS["muted"]
        ).pack(anchor="w")

        self.publisher_topic = ctk.CTkComboBox(
            body,
            values=["Connect first"],
            width=400,
            height=38,
            state="disabled"
        )
        self.publisher_topic.pack(anchor="w", pady=(5, 12))

        ctk.CTkLabel(
            body,
            text="Announcement Title",
            font=("Helvetica", 12),
            text_color=COLORS["muted"]
        ).pack(anchor="w")

        self.publisher_title = ctk.CTkEntry(
            body,
            width=550,
            height=38,
            placeholder_text="Enter announcement title"
        )
        self.publisher_title.pack(anchor="w", pady=(5, 12))

        ctk.CTkLabel(
            body,
            text="Announcement Content",
            font=("Helvetica", 12),
            text_color=COLORS["muted"]
        ).pack(anchor="w")

        self.publisher_content = ctk.CTkTextbox(
            body,
            width=550,
            height=110,
            font=("Helvetica", 13)
        )
        self.publisher_content.pack(anchor="w", pady=(5, 12))

        # ACTION BUTTONS
        actions = ctk.CTkFrame(
            body,
            fg_color="transparent"
        )
        actions.pack(fill="x", pady=8)

        self.publisher_send_button = ctk.CTkButton(
            actions,
            text="Publish Announcement",
            width=190,
            height=38,
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"],
            command=self.publish_announcement,
            state="disabled"
        )
        self.publisher_send_button.pack(side="left", padx=(0, 10))

        self.publisher_history_button = ctk.CTkButton(
            actions,
            text="View History",
            width=130,
            height=38,
            fg_color=COLORS["navy"],
            hover_color=COLORS["navy_light"],
            command=self.show_publisher_history,
            state="disabled"
        )
        self.publisher_history_button.pack(side="left")

        self.publisher_result = ctk.CTkTextbox(
            body,
            width=550,
            height=130,
            font=("Menlo", 11)
        )
        self.publisher_result.pack(fill="x", pady=(10, 15))

        self.publisher_result.insert(
            "end",
            "Publisher activity will appear here."
        )
        self.publisher_result.configure(state="disabled")

    def connect_publisher(self):

        if self.publisher_socket is not None:
            return

        selected_name = self.publisher_office.get()
        entered_id = self.publisher_id_entry.get().strip()

        publisher_id = None

        for key, profile in PUBLISHER_PROFILES.items():
            if profile["name"] == selected_name:
                publisher_id = key
                break

        if publisher_id is None:
            messagebox.showerror(
                "Invalid Office",
                "Please select a valid university office."
            )
            return

        if entered_id != publisher_id:
            messagebox.showerror(
                "Access Denied",
                "The Publisher ID does not match the selected office."
            )
            return

        sock = None

        try:
            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )
            sock.settimeout(5)
            sock.connect(("127.0.0.1", 5000))

            send_msg(
                sock,
                create_message(
                    REGISTER_PUBLISHER,
                    client_id=publisher_id
                )
            )

            response = recv_msg(sock)

            if (
                response is None
                or response.get("type") != STATUS
                or response.get("status") != "OK"
            ):
                sock.close()

                messagebox.showerror(
                    "Registration Failed",
                    "The broker did not accept your registration."
                )
                return

            sock.settimeout(None)

            self.publisher_socket = sock
            self.publisher_id = publisher_id

            topics = sorted(
                get_publisher_topics(publisher_id)
            )

            self.publisher_topic.configure(
                values=topics,
                state="readonly"
            )

            if topics:
                self.publisher_topic.set(topics[0])

            self.publisher_status.configure(
                text=f"● Connected as {selected_name}",
                text_color=COLORS["green"]
            )

            self.publisher_connect_button.configure(
                text="Connected",
                state="disabled"
            )

            self.publisher_disconnect_button.configure(
                state="normal"
            )

            self.publisher_office.configure(state="disabled")
            self.publisher_id_entry.configure(state="disabled")

            self.publisher_send_button.configure(state="normal")
            self.publisher_history_button.configure(state="normal")

            self.add_log(
                "INFO",
                f"Publisher {publisher_id} connected."
            )

        except (OSError, ConnectionError, ValueError) as exc:

            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

            messagebox.showerror(
                "Connection Error",
                f"Could not connect to UniSync:\n{exc}"
            )

    def disconnect_publisher(self, log=True):

        sock = self.publisher_socket
        was_connected = sock is not None

        if sock is not None:

            try:
                send_msg(
                    sock,
                    create_message(DISCONNECT)
                )
            except (OSError, ConnectionError, ValueError):
                pass

            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                sock.close()
            except OSError:
                pass

        self.publisher_socket = None
        self.publisher_id = None

        if (
            self.publisher_window is not None
            and self.publisher_window.winfo_exists()
        ):

            self.publisher_status.configure(
                text="● Not connected",
                text_color=COLORS["red"]
            )

            self.publisher_connect_button.configure(
                text="Connect to UniSync",
                state="normal"
            )

            self.publisher_disconnect_button.configure(
                state="disabled"
            )

            self.publisher_office.configure(state="normal")
            self.publisher_id_entry.configure(state="normal")

            self.publisher_topic.configure(
                values=["Connect first"],
                state="disabled"
            )
            self.publisher_topic.set("Connect first")

            self.publisher_send_button.configure(state="disabled")
            self.publisher_history_button.configure(state="disabled")

        if was_connected and log:
            self.add_log("INFO", "Publisher disconnected.")

    def publish_announcement(self):

        if self.publisher_socket is None:
            messagebox.showwarning(
                "Not Connected",
                "Connect to UniSync first."
            )
            return

        topic = self.publisher_topic.get()
        title = self.publisher_title.get().strip()
        content = self.publisher_content.get("1.0", "end").strip()

        if not title or not content:
            messagebox.showwarning(
                "Incomplete Announcement",
                "Please enter both a title and announcement content."
            )
            return

        if topic not in get_publisher_topics(self.publisher_id):
            messagebox.showerror(
                "Invalid Topic",
                "This topic is not authorized for your office."
            )
            return

        try:
            send_msg(
                self.publisher_socket,
                create_message(
                    PUBLISH,
                    topic=topic,
                    title=title,
                    content=content
                )
            )

            response = recv_msg(self.publisher_socket)

            if response is None:
                raise ConnectionError(
                    "Broker connection was closed."
                )

            if response.get("type") == ERROR:
                messagebox.showerror(
                    "Publishing Failed",
                    response.get("reason", "Unknown broker error.")
                )
                return

            if (
                response.get("type") == STATUS
                and response.get("status") == "ACCEPTED"
            ):
                message_id = response.get("message_id", "N/A")

                self.publisher_result.configure(state="normal")

                self.publisher_result.insert(
                    "end",
                    f"\n\n[PUBLISHED]\n"
                    f"Topic: {topic}\n"
                    f"Title: {title}\n"
                    f"Message ID: {message_id}\n"
                )

                self.publisher_result.see("end")
                self.publisher_result.configure(state="disabled")

                self.publisher_title.delete(0, "end")
                self.publisher_content.delete("1.0", "end")

                self.add_log(
                    "INFO",
                    f"Announcement published: {title}"
                )

            else:
                messagebox.showerror(
                    "Unexpected Response",
                    f"Broker response: {response}"
                )

        except (OSError, ConnectionError, ValueError) as exc:
            messagebox.showerror(
                "Publishing Error",
                str(exc)
            )

    def show_publisher_history(self):

        if self.publisher_socket is None:
            messagebox.showwarning(
                "Not Connected",
                "Connect to UniSync first."
            )
            return

        try:
            send_msg(
                self.publisher_socket,
                create_message(HISTORY)
            )

            response = recv_msg(self.publisher_socket)

            if response is None:
                raise ConnectionError(
                    "Broker connection was closed."
                )

            if response.get("type") == ERROR:
                messagebox.showerror(
                    "History Error",
                    response.get("reason", "Could not retrieve history.")
                )
                return

            if response.get("type") != HISTORY:
                messagebox.showerror(
                    "Unexpected Response",
                    f"Broker response: {response}"
                )
                return

            messages = response.get("messages", [])

            self.publisher_result.configure(state="normal")
            self.publisher_result.delete("1.0", "end")

            self.publisher_result.insert(
                "end",
                "===== PUBLISHED MESSAGE HISTORY =====\n"
            )

            if not messages:
                self.publisher_result.insert(
                    "end",
                    "\nNo published messages yet."
                )

            for message in messages:
                self.publisher_result.insert(
                    "end",
                    "\n----------------------------------\n"
                    f"Topic: {message.get('topic')}\n"
                    f"Title: {message.get('title')}\n"
                    f"Content: {message.get('content')}\n"
                    f"Message ID: {message.get('message_id')}\n"
                )

            self.publisher_result.see("1.0")
            self.publisher_result.configure(state="disabled")

        except (OSError, ConnectionError, ValueError) as exc:
            messagebox.showerror(
                "History Error",
                str(exc)
            )

    def close_publisher_window(self):

        self.disconnect_publisher(log=False)

        if self.publisher_window is not None:
            self.publisher_window.destroy()
            self.publisher_window = None

        self.add_log("INFO", "Publisher window closed.")

    # ==========================================
    # SUBSCRIBER WINDOW
    # ==========================================

    def open_subscriber(self):

        if self.subscriber_window is not None:
            if self.subscriber_window.winfo_exists():
                self.subscriber_window.focus()
                return

        self.subscriber_window = ctk.CTkToplevel(self)
        self.subscriber_window.title("UniSync - Subscriber")
        self.subscriber_window.geometry("850x650")
        self.subscriber_window.configure(
            fg_color=COLORS["background"]
        )

        self.subscriber_window.protocol(
            "WM_DELETE_WINDOW",
            self.close_subscriber_window
        )

        # HEADER
        header = ctk.CTkFrame(
            self.subscriber_window,
            fg_color=COLORS["navy"],
            corner_radius=0
        )
        header.pack(fill="x")

        ctk.CTkLabel(
            header,
            text="SUBSCRIBER DASHBOARD",
            font=("Arial", 22, "bold"),
            text_color=COLORS["white"]
        ).pack(anchor="w", padx=25, pady=(20, 5))

        ctk.CTkLabel(
            header,
            text="Connect, manage topics, and receive university announcements.",
            font=("Arial", 12),
            text_color="#D5E1E3"
        ).pack(anchor="w", padx=25, pady=(0, 20))

        # MAIN CONTENT
        content = ctk.CTkFrame(
            self.subscriber_window,
            fg_color=COLORS["background"]
        )
        content.pack(
            fill="both",
            expand=True,
            padx=25,
            pady=20
        )

        # REGISTRATION
        ctk.CTkLabel(
            content,
            text="Subscriber Registration",
            font=("Arial", 16, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", pady=(0, 10))

        self.subscriber_id_entry = ctk.CTkEntry(
            content,
            placeholder_text="Enter your University Number",
            height=38
        )
        self.subscriber_id_entry.pack(fill="x", pady=(0, 10))

        self.subscriber_connect_button = ctk.CTkButton(
            content,
            text="Connect to UniSync",
            command=self.connect_subscriber,
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"],
            height=38
        )
        self.subscriber_connect_button.pack(anchor="w")

        self.subscriber_status_label = ctk.CTkLabel(
            content,
            text="● Not connected",
            text_color=COLORS["red"]
        )
        self.subscriber_status_label.pack(
            anchor="w",
            pady=10
        )

        # TOPIC MANAGEMENT
        ctk.CTkLabel(
            content,
            text="Topic Management",
            font=("Arial", 16, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", pady=(15, 10))

        self.subscriber_topic_menu = ctk.CTkOptionMenu(
            content,
            values=list(ALL_TOPICS.values()),
            width=350,
            state="disabled"
        )
        self.subscriber_topic_menu.pack(
            anchor="w",
            pady=(0, 10)
        )

        topic_buttons = ctk.CTkFrame(
            content,
            fg_color="transparent"
        )
        topic_buttons.pack(anchor="w")

        self.subscriber_subscribe_button = ctk.CTkButton(
            topic_buttons,
            text="Subscribe",
            command=self.subscribe_gui,
            state="disabled",
            fg_color=COLORS["teal"],
            hover_color=COLORS["teal_hover"]
        )
        self.subscriber_subscribe_button.pack(
            side="left",
            padx=(0, 8)
        )

        self.subscriber_unsubscribe_button = ctk.CTkButton(
            topic_buttons,
            text="Unsubscribe",
            command=self.unsubscribe_gui,
            state="disabled",
            fg_color=COLORS["navy"],
            hover_color=COLORS["navy_light"]
        )
        self.subscriber_unsubscribe_button.pack(
            side="left",
            padx=8
        )

        self.subscriber_history_button = ctk.CTkButton(
            content,
            text="View Received Message History",
            command=self.view_subscriber_history,
            state="disabled",
            fg_color=COLORS["navy"],
            hover_color=COLORS["navy_light"]
        )
        self.subscriber_history_button.pack(
            anchor="w",
            pady=15
        )

        # NOTIFICATIONS AND ACTIVITY
        ctk.CTkLabel(
            content,
            text="Notifications and Activity",
            font=("Arial", 16, "bold"),
            text_color=COLORS["navy"]
        ).pack(anchor="w", pady=(5, 8))

        self.subscriber_output = ctk.CTkTextbox(
            content,
            height=180,
            corner_radius=8
        )
        self.subscriber_output.pack(
            fill="both",
            expand=True
        )

        self.subscriber_output.insert(
            "end",
            "Welcome to UniSync Subscriber.\n"
            "Connect using your University Number to get started.\n"
        )
        self.subscriber_output.configure(state="disabled")

        self.process_subscriber_events()

    def connect_subscriber(self):

        client_id = self.subscriber_id_entry.get().strip()

        if not client_id:
            messagebox.showwarning(
                "Missing University Number",
                "Please enter your University Number."
            )
            return

        try:
            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )
            sock.settimeout(5)
            sock.connect(("127.0.0.1", 5000))

            send_msg(
                sock,
                create_message(
                    REGISTER_SUBSCRIBER,
                    client_id=client_id
                )
            )

            response = recv_msg(sock)

            if response is None or response.get("type") != STATUS:
                sock.close()

                messagebox.showerror(
                    "Registration Failed",
                    f"Broker response: {response}"
                )
                return

            sock.settimeout(None)

            self.subscriber_socket = sock
            self.subscriber_id = client_id
            self.subscriber_stop_event = threading.Event()

            self.subscriber_listener_thread = threading.Thread(
                target=self.listen_for_subscriber_messages,
                daemon=True
            )
            self.subscriber_listener_thread.start()

            self.subscriber_status_label.configure(
                text=f"● Connected as {client_id}",
                text_color=COLORS["green"]
            )

            self.subscriber_connect_button.configure(
                text="Connected",
                state="disabled"
            )

            self.subscriber_id_entry.configure(state="disabled")
            self.subscriber_topic_menu.configure(state="normal")
            self.subscriber_subscribe_button.configure(state="normal")
            self.subscriber_unsubscribe_button.configure(state="normal")
            self.subscriber_history_button.configure(state="normal")

            self.subscriber_output.configure(state="normal")
            self.subscriber_output.insert(
                "end",
                f"\n[CONNECTED] Subscriber {client_id} connected.\n"
            )
            self.subscriber_output.configure(state="disabled")

            self.add_log(
                "INFO",
                f"Subscriber {client_id} connected."
            )

        except (OSError, ConnectionError, ValueError) as exc:
            messagebox.showerror(
                "Connection Error",
                f"Could not connect to UniSync:\n{exc}"
            )

    def listen_for_subscriber_messages(self):

        sock = self.subscriber_socket
        stop_event = self.subscriber_stop_event

        while not stop_event.is_set():

            try:
                response = recv_msg(sock)

                if response is None:
                    self.subscriber_ui_queue.put(
                        ("disconnected", "Broker connection closed.")
                    )
                    break

                message_type = response.get("type")

                if message_type == MESSAGE:

                    self.subscriber_ui_queue.put(
                        ("message", response)
                    )

                    message_id = response.get("message_id")

                    if isinstance(message_id, str) and message_id:
                        try:
                            with self.subscriber_send_lock:
                                send_msg(
                                    sock,
                                    create_message(
                                        ACK,
                                        message_id=message_id
                                    )
                                )

                        except (OSError, ConnectionError):
                            self.subscriber_ui_queue.put(
                                ("disconnected", "Could not acknowledge notification.")
                            )
                            break

                elif message_type in (STATUS, ERROR, HISTORY):
                    self.subscriber_response_queue.put(response)

            except (OSError, ConnectionError, ValueError) as exc:

                if not stop_event.is_set():
                    self.subscriber_ui_queue.put(
                        ("disconnected", str(exc))
                    )

                break

        stop_event.set()

    def process_subscriber_events(self):

        if (
            self.subscriber_window is None
            or not self.subscriber_window.winfo_exists()
        ):
            return

        while not self.subscriber_ui_queue.empty():

            event_type, data = self.subscriber_ui_queue.get()

            self.subscriber_output.configure(state="normal")

            if event_type == "message":

                self.subscriber_output.insert(
                    "end",
                    "\n===== NEW NOTIFICATION =====\n"
                    f"Topic: {data.get('topic')}\n"
                    f"Title: {data.get('title')}\n"
                    f"Content: {data.get('content')}\n"
                    f"Message ID: {data.get('message_id')}\n"
                    "Notification acknowledged.\n"
                    "============================\n"
                )

            elif event_type == "request":

                action = data["action"]
                response = data["response"]

                if (
                    action == "history"
                    and response.get("type") == HISTORY
                ):

                    messages = response.get("messages", [])

                    self.subscriber_output.insert(
                        "end",
                        "\n===== RECEIVED MESSAGE HISTORY =====\n"
                    )

                    if not messages:
                        self.subscriber_output.insert(
                            "end",
                            "No received messages yet.\n"
                        )

                    for message in messages:
                        self.subscriber_output.insert(
                            "end",
                            "\n------------------------------\n"
                            f"Topic: {message.get('topic')}\n"
                            f"Title: {message.get('title')}\n"
                            f"Content: {message.get('content')}\n"
                            f"Message ID: {message.get('message_id')}\n"
                        )

                elif response.get("type") == STATUS:

                    self.subscriber_output.insert(
                        "end",
                        f"\n[{action.upper()}] "
                        f"{response.get('status', '')}\n"
                    )

                else:
                    self.subscriber_output.insert(
                        "end",
                        f"\n[{action.upper()} FAILED] "
                        f"{response.get('reason', response)}\n"
                    )

            elif event_type == "request_error":

                self.subscriber_output.insert(
                    "end",
                    f"\n[REQUEST ERROR] {data['reason']}\n"
                )

            elif event_type == "disconnected":

                self.subscriber_output.insert(
                    "end",
                    f"\n[DISCONNECTED] {data}\n"
                )

                self.subscriber_status_label.configure(
                    text="● Disconnected",
                    text_color=COLORS["red"]
                )

                self.subscriber_connect_button.configure(
                    text="Disconnected",
                    state="disabled"
                )

                self.subscriber_subscribe_button.configure(state="disabled")
                self.subscriber_unsubscribe_button.configure(state="disabled")
                self.subscriber_history_button.configure(state="disabled")
                self.subscriber_topic_menu.configure(state="disabled")

            self.subscriber_output.see("end")
            self.subscriber_output.configure(state="disabled")

        self.subscriber_window.after(
            200,
            self.process_subscriber_events
        )

    def start_subscriber_request(self, request, action):

        if self.subscriber_socket is None:
            messagebox.showwarning(
                "Not Connected",
                "Connect to UniSync first."
            )
            return

        sock = self.subscriber_socket

        def send_request():

            with self.subscriber_request_lock:

                try:
                    with self.subscriber_send_lock:
                        send_msg(sock, request)

                    response = self.subscriber_response_queue.get(
                        timeout=5
                    )

                    self.subscriber_ui_queue.put(
                        ("request", {
                            "action": action,
                            "response": response
                        })
                    )

                except queue.Empty:
                    self.subscriber_ui_queue.put(
                        ("request_error", {
                            "action": action,
                            "reason": "No response from broker."
                        })
                    )

                except (OSError, ConnectionError, ValueError) as exc:
                    self.subscriber_ui_queue.put(
                        ("request_error", {
                            "action": action,
                            "reason": str(exc)
                        })
                    )

        threading.Thread(
            target=send_request,
            daemon=True
        ).start()

    def subscribe_gui(self):

        if self.subscriber_socket is None:
            return

        topic = self.subscriber_topic_menu.get()

        self.start_subscriber_request(
            create_message(SUBSCRIBE, topic=topic),
            "subscribe"
        )

    def unsubscribe_gui(self):

        if self.subscriber_socket is None:
            return

        topic = self.subscriber_topic_menu.get()

        self.start_subscriber_request(
            create_message(UNSUBSCRIBE, topic=topic),
            "unsubscribe"
        )

    def view_subscriber_history(self):

        if self.subscriber_socket is None:
            return

        self.start_subscriber_request(
            create_message(HISTORY),
            "history"
        )

    def close_subscriber_window(self):

        if self.subscriber_socket is not None:

            try:
                with self.subscriber_send_lock:
                    send_msg(
                        self.subscriber_socket,
                        create_message(DISCONNECT)
                    )

            except (OSError, ConnectionError, ValueError):
                pass

            if self.subscriber_stop_event is not None:
                self.subscriber_stop_event.set()

            try:
                self.subscriber_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            self.subscriber_socket.close()
            self.subscriber_socket = None
            self.subscriber_id = None

        if self.subscriber_window is not None:
            self.subscriber_window.destroy()
            self.subscriber_window = None

        self.add_log("INFO", "Subscriber disconnected.")


if __name__ == "__main__":
    app = UniSyncApp()
    app.mainloop()
