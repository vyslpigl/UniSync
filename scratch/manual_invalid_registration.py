import socket
import json
import struct

HOST = "127.0.0.1"
PORT = 5000

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

# Send a validly framed JSON message,
# but with an unsupported message type.
message = {
    "type": "HELLO",
    "client_id": "INVALID-CLIENT"
}

payload = json.dumps(message).encode("utf-8")
frame = struct.pack("!I", len(payload)) + payload

sock.sendall(frame)

# Read the broker's response header (4 bytes)
header = sock.recv(4)

if header:
    length = struct.unpack("!I", header)[0]
    response = sock.recv(length)
    print("Broker response:", response.decode("utf-8"))
else:
    print("Broker closed the connection without a response.")

# Check if the broker closed the connection
print("Next receive:", sock.recv(1))

sock.close()