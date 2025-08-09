# shared/tcp_client.py
import socket

def request_fragment(fragment_name, host, port=5001):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(f"GET_FRAGMENT {fragment_name}".encode())
        data = b""
        while True:
            chunk = s.recv(1024)
            if not chunk:
                break
            data += chunk
        with open(f"downloaded/{fragment_name}", 'wb') as f:
            f.write(data)
        print(f"[TCP Client] Fragmento {fragment_name} recibido.")
