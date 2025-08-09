import socket
import os
import threading
import requests
from flask import Flask, jsonify, request

# Carpeta donde se almacenan los fragmentos
FRAGMENTS_DIR = "fragments"
# URL para enviar fragmentos al nodo2
NODE2_UPLOAD_URL = "http://node2:8011/upload_fragment"

# Esta funcion inicia el servidor TCP que escucha peticiones para enviar fragmentos
def start_server(port=5001):
    # Creación del socket TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Asocia el socket a la dirección y puerto
        s.bind(('', port))
        # Escucha conexiones entrantes
        s.listen()
        print(f"[TCP Server] Escuchando en puerto {port}")
        # Acepta conexiones entrantes
        while True:
            conn, addr = s.accept() #esperar y aceptar las conexiones
            with conn:
                data = conn.recv(1024).decode() #recibir datos del cliente
                print(f"[TCP Server] Petición recibida: {data}")
                # Si es para pedir un fragmento 
                if data.startswith("GET_FRAGMENT"):
                    _, fragment_name = data.split() #Obtener el nombre del fragmento
                    file_path = os.path.join(FRAGMENTS_DIR, fragment_name)
                    # Verificar si el fragmento existe
                    if os.path.exists(file_path):
                        # Enviar el fragmento al cliente
                        with open(file_path, 'rb') as f:
                            conn.sendall(f.read())
                    else:
                        # Enviar un mensaje de error si el fragmento no se encuentra
                        conn.sendall(b"ERROR: Fragmento no encontrado")

# Crear la aplicación Flask
app = Flask(__name__)

# Ruta raiz que muestra el estado del nodo
@app.route('/')
def home():
    return "Nodo1 funcionando correctamente"

# Ruta que lista todos los fragmentos disponibles
@app.route("/fragments")
def list_fragments():
    files = os.listdir(FRAGMENTS_DIR)
    return jsonify(files)

# Nueva ruta: envía todos los fragmentos al nodo2
@app.route("/send_all_fragments")
def send_all_fragments():
    files = os.listdir(FRAGMENTS_DIR)
    success, failed = [], []
    # Para cada archivo intentar subirlo al nodo2
    for filename in files:
        filepath = os.path.join(FRAGMENTS_DIR, filename)
        try:
            with open(filepath, 'rb') as f:
                #Enviar un archivo al nodo2 usando HTTP
                response = requests.post(NODE2_UPLOAD_URL, files={'file': (filename, f)})
                # Revisar la respuesta
                if response.status_code == 200:
                    print(f"[UPLOAD] {filename} → OK")
                    success.append(filename)
                else:
                    print(f"[UPLOAD] {filename} → FAILED")
                    failed.append(filename)
        except Exception as e:
            print(f"[ERROR] {filename}: {e}")
            failed.append(filename)
            
    # Responder con el resultado de la operación
    return {
        "enviados": success,
        "fallidos": failed
    }

# Endpoint para recibir fragmentos via POST desde nodo2
@app.route('/upload_fragment', methods=['POST'])
def upload_fragment():
    file = request.files.get('file')  # Obtener el archivo del request
    if not file:
        return "No file uploaded", 400
    # Guarda el fragmento en la carpeta correspondiente
    save_path = os.path.join(FRAGMENTS_DIR, file.filename)
    file.save(save_path)
    print(f"[UPLOAD] Fragmento recibido: {file.filename}")
    return f"{file.filename} recibido correctamente", 200

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
    # Lanzar servidor TCP en un hilo
    tcp_thread = threading.Thread(target=start_server, daemon=True)
    tcp_thread.start()

    # Ejecutar el servidor Flask para exponer la api 
    app.run(host="0.0.0.0", port=8010)
