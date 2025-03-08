import firebase_admin
from firebase_admin import credentials, db
import requests
import requests
from tqdm import tqdm
import csv
import time
import os

# 🔹 1️⃣ Inicializar Firebase
cred_path = r'C:\Users\Carlo\Desktop\Proyectos\data\bubbo-data\Test\bubbo-dfba0-firebase-adminsdk-fbsvc-ec66b76bce.json'
cred = credentials.Certificate(cred_path)

# Inicializa Firebase solo si no está ya inicializado
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app'
    })

print("✅ Firebase inicializado correctamente.")


# En esta celda obtengo el listado de todos los documentos con sus nombres
url = "https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app/documents.json?shallow=true"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    document_ids = list(data.keys())  # Lista con los nombres de los documentos
    print(f"Total de documentos: {len(document_ids)}")
    print("Primeros 10 documentos:", document_ids[:10])
else:
    print(f"Error {response.status_code}: {response.text}")


# 🔹 2️⃣ Definir la URL base de Firebase
BASE_URL = "https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app/documents/"


document_list = []
for document in document_ids:
    url = f'{BASE_URL}{document}'
    document_list.append(url)

# URL base de la API
BASE_URL = "https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app/documents/"

# Archivo CSV de salida
output_file = 'extracted_data.csv'
fieldnames = ["ID", "Genre", "CleanTitle", "Synopsis", "Directors", "Cast", "Type", "PlatformName"]

# Archivo para almacenar las URLs ya procesadas
progress_file = "processed_urls.txt"

# Cargar el progreso anterior (documentos ya procesados)
processed_urls = set()
if os.path.exists(progress_file):
    with open(progress_file, "r", encoding="utf-8") as f:
        processed_urls = set(line.strip() for line in f.readlines())

# Si el archivo CSV no existe, escribir los encabezados
if not os.path.exists(output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

# Función para descargar y extraer datos de un documento
def fetch_and_extract_data(url):
    document_id = url.split("documents/")[1]
    full_url = f'{BASE_URL}{document_id}.json'

    print(f"📡 Descargando: {full_url}")  

    retries = 5
    delay = 1
    extracted_records = []

    for attempt in range(retries):
        try:
            response = requests.get(full_url, timeout=10)  # ⏳ Timeout agregado
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"📄 {len(data)} registros obtenidos de {document_id}")  

                    for record_id, record_data in data.items():
                        content = record_data.get("content", {})

                        # Extraer valores con manejo de datos faltantes
                        genres = "; ".join(content.get("Genres", [])) if content.get("Genres") else "No Genre"
                        clean_title = content.get("CleanTitle", "No Title")
                        synopsis = content.get("Synopsis", "No Synopsis")
                        directors = "; ".join(content.get("Directors", [])) if content.get("Directors") else "No Directors"
                        cast = "; ".join(content.get("Cast", [])) if content.get("Cast") else "No Cast"
                        movie_type = content.get("Type", "No Type")
                        platform_name = content.get("PlatformName", "No Platform")

                        extracted_records.append({
                            "ID": record_id,
                            "Genre": genres,
                            "CleanTitle": clean_title,
                            "Synopsis": synopsis,
                            "Directors": directors,
                            "Cast": cast,
                            "Type": movie_type,
                            "PlatformName": platform_name
                        })

                    return extracted_records  

                except ValueError:
                    print(f"⚠️ Error al convertir JSON en {full_url}")
            else:
                print(f"❌ Error {response.status_code} en {full_url}")

        except requests.exceptions.RequestException as e:
            print(f"❌ Excepción en {full_url}: {e}")

        print(f"🔁 Reintentando en {delay} segundos... ({attempt+1}/{retries})")
        time.sleep(delay)
        delay *= 2  

    return None

# 🔄 Procesar cada documento uno por uno, **saltando los ya procesados**
with open(output_file, mode='a', newline='', encoding='utf-8') as file, open(progress_file, mode="a", encoding="utf-8") as progress:
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    for url in tqdm(document_list, desc="Procesando documentos"):
        if url in processed_urls:
            print(f"✅ Ya procesado, saltando: {url}")
            continue

        results = fetch_and_extract_data(url)
        if results:
            writer.writerows(results)
            file.flush()  # 📝 Forzar escritura en disco
            progress.write(url + "\n")  # Guardar progreso
            progress.flush()  # Asegurar que se guarda

print(f"✅ Datos extraídos y guardados en {output_file}")