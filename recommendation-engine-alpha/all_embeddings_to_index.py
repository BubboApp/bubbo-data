!Revisar codigo en ipynb antes de ejecutarlo
import os
import json
from google.cloud import storage

# Configuraciones
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bubbo-dfba0-47e395cdcdc7.json"
PROJECT = "bubbo-dfba0"
BUCKET_NAME = "embeddings_new_bucket"
GCS_PREFIX = "embeddings/movies_and_series"
OUTPUT_FILE = "/tmp/all_embeddings.jsonl"
GCS_OUTPUT_PATH = "embeddings/index_data/all_embeddings.jsonl"

# Inicializar cliente
storage_client = storage.Client(project=PROJECT)
bucket = storage_client.bucket(BUCKET_NAME)

# Descargar todos los JSON y convertir a JSONL
blobs = bucket.list_blobs(prefix=GCS_PREFIX)

with open(OUTPUT_FILE, "w") as jsonl_file:
    for blob in blobs:
        if blob.name.endswith(".json"):
            content = blob.download_as_text()
            data = json.loads(content)

            if "id" in data and "embedding" in data:
                jsonl_file.write(json.dumps(data) + "\n")

# Subir el archivo consolidado a GCS
output_blob = bucket.blob(GCS_OUTPUT_PATH)
output_blob.upload_from_filename(OUTPUT_FILE)
print(f"Archivo de embeddings consolidado subido a: gs://{BUCKET_NAME}/{GCS_OUTPUT_PATH}")
