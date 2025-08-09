import subprocess
import os
import requests

def split_video_ffmpeg(video_path, output_dir, parts=10):
    os.makedirs(output_dir, exist_ok=True)
    
    result = subprocess.run([
        'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1', video_path
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    duration = float(result.stdout)
    part_duration = duration / parts
    
    print(f"Duración total: {duration:.2f} s, fragmentos: {parts}, duración por fragmento: {part_duration:.2f} s")

    fragment_paths = []

    for i in range(parts):
        start = i * part_duration
        output_file = os.path.join(output_dir, f"fragment_{i}.mp4")
        
        subprocess.run([
            'ffmpeg',
            '-ss', str(start),
            '-i', video_path,
            '-t', str(part_duration),
            '-c', 'copy',
            '-y',  
            output_file
        ])
        
        print(f"Guardado fragmento {i}: {output_file}")
        fragment_paths.append(output_file)
    
    return fragment_paths

def send_fragment(file_path, url):
    with open(file_path, 'rb') as f:
        response = requests.post(url, files={'file': (os.path.basename(file_path), f)})
        if response.status_code == 200:
            print(f"[✓] Enviado correctamente → {os.path.basename(file_path)}")
        else:
            print(f"[✗] Error al enviar {os.path.basename(file_path)} → {response.status_code}")

if __name__ == "__main__":
    # Configura las URLs de los nodos
    NODE1_URL = "http://localhost:8010/upload_fragment"
    NODE2_URL = "http://localhost:8011/upload_fragment"

    # Divide el video
    fragments = split_video_ffmpeg("3770775687-preview.mp4", "fragments", parts=10)

    # Enviar mitad a nodo1, mitad a nodo2
    for idx, frag_path in enumerate(fragments):
        if idx < len(fragments) // 2:
            send_fragment(frag_path, NODE1_URL)
        else:
            send_fragment(frag_path, NODE2_URL)

