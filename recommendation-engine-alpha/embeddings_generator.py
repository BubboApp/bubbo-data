import vertexai
from vertexai.language_models import TextEmbeddingModel
from google.cloud import bigquery, storage
import pandas as pd
import json
import os
import time


# Configuración
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../../bubbo-dfba0-47e395cdcdc7.json"
PROJECT = "bubbo-dfba0"
LOCATION = "europe-southwest1"
BUCKET_NAME = "embeddings_new_bucket"
GCS_PREFIX = "embeddings/movies_and_series"
BQ_QUERY = """
    SELECT tmdb_id AS ID, title, genres, synopsis 
    FROM `bubbo-dfba0.content.best_content_translated_py`
    LIMIT 500000
    OFFSET 48615
"""
EMBEDDING_MODEL = "text-multilingual-embedding-002"
BATCH_SIZE = 11  # Solo 10 registros

# Inicializar APIs
vertexai.init(project=PROJECT, location=LOCATION)
bq_client = bigquery.Client(project=PROJECT)
storage_client = storage.Client(project=PROJECT)

# Cargar el modelo de embeddings
try:
    embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
    print("Modelo de embeddings cargado correctamente.")
except Exception as e:
    print(f"Error al cargar el modelo de embeddings: {e}")
    raise

def get_data_from_bigquery():
    print("Ejecutando la consulta en BigQuery...")
    job = bq_client.query(BQ_QUERY)
    iterator = job.result()
    rows = list(iterator)

    if not rows:
        print("No se encontraron resultados.")
        return [], []

    data = [{"ID": row["ID"], "title": row["title"], "genres": row["genres"], "synopsis": row["synopsis"]} for row in rows]
    df = pd.DataFrame(data)

    return df['ID'].astype(str).tolist(), df['title'].astype(str).tolist()

def create_embeddings(texts):
    embeddings = embedding_model.get_embeddings(texts)
    return [embedding.values for embedding in embeddings]

def upload_embeddings_to_gcs(ids, titles):
    for id, text in zip(ids, titles):
        gcs_path = f"{GCS_PREFIX}/{id}.json"
        blob = storage_client.bucket(BUCKET_NAME).blob(gcs_path)

        # Validar si el archivo ya existe en GCS
        if blob.exists():
            print(f"El embedding de ID {id} ya existe en GCS. Saltando...")
            continue

        # Si no existe, generar y subir
        embedding = embedding_model.get_embeddings([text])[0].values
        time.sleep(0.4)
        with open(f"/tmp/{id}.json", "w") as f:
            json.dump({"id": id, "embedding": embedding}, f)

        blob.upload_from_filename(f"/tmp/{id}.json")
        print(f"Embedding de ID {id} subido a: {gcs_path}")

# MAIN
if __name__ == "__main__":
    ids, titles = get_data_from_bigquery()
    
    if ids:
        upload_embeddings_to_gcs(ids, titles)
    else:
        print("No se encontraron IDs para procesar.")
