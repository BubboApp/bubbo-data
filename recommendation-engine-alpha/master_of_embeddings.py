import os
import json
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

# Configuraciones
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bubbo-dfba0-47e395cdcdc7.json"
PROJECT = "bubbo-dfba0"
BUCKET_NAME = "embeddings_new_bucket"
GCS_PREFIX = "embeddings/movies_and_series"
OUTPUT_FILE = "/tmp/all_embeddings.jsonl"  # Archivo temporal local
GCS_OUTPUT_PATH = "embeddings/index_data/all_embeddings.jsonl"

# Inicializar cliente
storage_client = storage.Client(project=PROJECT)
bucket = storage_client.bucket(BUCKET_NAME)

# Leer archivos existentes para evitar reprocesar
existing_ids = set()
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r") as f:
        lines = f.readlines()
        for line in lines:
            if line.strip():  # Ignorar líneas vacías
                try:
                    obj = json.loads(line)
                    id_ = obj.get("id")
                    if id_:
                        existing_ids.add(id_)
                except json.JSONDecodeError:
                    continue

print(f"Se encontraron {len(existing_ids)} IDs ya procesados (evitar duplicados).")

# Función para procesar cada blob
errores = []

def process_blob(blob):
    if not blob.name.endswith(".json"):
        return None
    try:
        content = blob.download_as_text()
        data = json.loads(content)
        id_ = data.get("id")
        embedding = data.get("embedding")

        if not id_ or not embedding:
            return None
        if id_ in existing_ids:
            return None

        return {"id": id_, "embedding": embedding}
    except Exception as e:
        errores.append({"file": blob.name, "error": str(e)})
        return None

# Descargar todos los JSONs en paralelo
blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))

nuevos = 0
counter = 0  # Contador para hacer el guardado cada 1000 JSON procesados

with open(OUTPUT_FILE, "a") as jsonl_file:
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(process_blob, blobs)

        for result in results:
            if result:
                jsonl_file.write(json.dumps(result) + "\n")
                existing_ids.add(result["id"])
                nuevos += 1
                counter += 1

                # Cada 1000 JSON procesados, guardar el progreso en el bucket
                if counter >= 1000:
                    output_blob = bucket.blob(GCS_OUTPUT_PATH)
                    output_blob.upload_from_filename(OUTPUT_FILE)
                    print(f"☁️ Guardado parcial de {counter} embeddings a GCS.")
                    counter = 0

# Subir el archivo final a GCS si hay nuevos datos
if nuevos > 0:
    output_blob = bucket.blob(GCS_OUTPUT_PATH)
    output_blob.upload_from_filename(OUTPUT_FILE)
    print(f"☁️ Archivo final subido a: gs://{BUCKET_NAME}/{GCS_OUTPUT_PATH}")
else:
    print("📦 No hubo nuevos embeddings, no se actualizó el archivo en GCS.")

print(f"✅ {nuevos} nuevos embeddings agregados.")
print(f"⚠️ {len(errores)} archivos fallaron.")

# (Opcional) Guardar errores en archivo local
if errores:
    with open("/tmp/errores_embed.json", "w") as f:
        json.dump(errores, f, indent=2)
    print("📝 Errores guardados en: /tmp/errores_embed.json")
