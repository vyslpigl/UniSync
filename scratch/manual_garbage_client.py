import socket
import struct
import time

HOST = "127.0.0.1"
PORT = 5000

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

# A frame with a 4-byte length header.
# The body contains invalid UTF-8 bytes.
garbage = b"\xff\xfe\xfa\xfb"

frame = struct.pack("!I", len(garbage)) + garbage

sock.sendall(frame)

# Give the broker time to process the bad data.
time.sleep(1)

try:
    data = sock.recv(1024)

    if data == b"":
        print("PASS: Broker closed the malformed client's connection.")
    else:
        print("Unexpected response:", data)

except ConnectionResetError:
    print("PASS: Malformed client connection was reset.")

sock.close()