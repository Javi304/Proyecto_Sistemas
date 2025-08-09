import socket
import os
import threading
import requests
from flask import Flask, jsonify, request

# Carpeta donde se almacenan los fragmentos
FRAGMENTS_DIR = "fragments"
# Crear la carpeta si no existe
os.makedirs(FRAGMENTS_DIR, exist_ok=True)
# URL para enviar fragmentos al nodo1
NODE1_UPLOAD_URL = "http://node1:8010/upload_fragment"

# Esta funcion inicia el servidor TCP que escucha peticiones para enviar fragmentos
def start_server(port=5002):
    # Creación del socket TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Asociar el socket a la dirección y puerto
        s.bind(('', port))
        # Escuchar conexiones entrantes
        s.listen()
        print(f"[TCP Server] Escuchando en puerto {port}")
        # Aceptar conexiones entrantes
        while True:
            conn, addr = s.accept() # Esperar y aceptar las conexiones
            with conn:
                # Recibir datos del cliente
                data = conn.recv(1024).decode()
                print(f"[TCP Server] Petición recibida: {data}")
                if data.startswith("GET_FRAGMENT"):
                    _, fragment_name = data.split() # Obtener el nombre del fragmento
                    file_path = os.path.join(FRAGMENTS_DIR, fragment_name) # Construir la ruta local
                    # Verificar si el fragmento existe
                    if os.path.exists(file_path):
                        # Abrir y enviar el fragmento
                        with open(file_path, 'rb') as f:
                            conn.sendall(f.read())
                    else:
                        # Enviar un mensaje de error si el fragmento no se encuentra
                        conn.sendall(b"ERROR: Fragmento no encontrado")

# Crear la aplicación Flask
app = Flask(__name__)

# Ruta principal para el estado del nodo
@app.route('/')
def home():
    return "Nodo2 funcionando correctamente"

# Ruta que lista todos los fragmentos disponibles
@app.route('/fragments')
def list_fragments():
    files = os.listdir(FRAGMENTS_DIR) # Obtener la lista de fragmentos
    return jsonify(files) # Enviar la lista de fragmentos como respuesta

# Endpoint para recibir fragmentos por POST
@app.route('/upload_fragment', methods=['POST'])
def upload_fragment():
    # Obtener el archivo del request
    file = request.files.get('file')
    if not file:
        return "No file uploaded", 400

    # Guardar el fragmento en la carpeta correspondiente
    save_path = os.path.join(FRAGMENTS_DIR, file.filename)
    file.save(save_path)
    print(f"[UPLOAD] Fragmento recibido: {file.filename}")
    return f"{file.filename} recibido correctamente", 200

# Endpoint para enviar todos los fragmentos al nodo1
@app.route('/send_all_fragments')
def send_all_fragments():
    success, failed = [], []
    # Recorrer todos los fragmentos
    for filename in os.listdir(FRAGMENTS_DIR):
        file_path = os.path.join(FRAGMENTS_DIR, filename)
        try:
            # Abrir el fragmento y enviarlo a nodo1
            with open(file_path, 'rb') as f:
                files = {'file': (filename, f)}
                # Enviar el fragmento a nodo1 mediante HTTP
                response = requests.post(NODE1_UPLOAD_URL, files=files)
                if response.status_code == 200:
                    success.append(filename)
                else:
                    failed.append(filename)
        except Exception as e:
            print(f"[ERROR] Enviando {filename}: {e}")
            failed.append(filename)
    # Devolver el resultado de la operación 
    return jsonify({"enviados": success, "fallidos": failed})

import shutil

@app.route('/delete_all_fragments', methods=['DELETE'])
def delete_all_fragments():
    try:
        files = os.listdir(FRAGMENTS_DIR)
        for f in files:
            os.remove(os.path.join(FRAGMENTS_DIR, f))
        return {"mensaje": "Todos los fragmentos eliminados"}, 200
    except Exception as e:
        return {"error": str(e)}, 500


if __name__ == "__main__":
    # Ejecutar el servidor TCP
    tcp_thread = threading.Thread(target=start_server, daemon=True)
    tcp_thread.start()

    # Ejecutar servidor Flask
    app.run(host="0.0.0.0", port=8011)