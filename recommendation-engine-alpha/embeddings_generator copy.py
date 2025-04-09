import vertexai
from vertexai.language_models import TextEmbeddingModel
from google.cloud import bigquery, storage, aiplatform
import pandas as pd
import json
import os
from uuid import uuid4

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../../bubbo-dfba0-47e395cdcdc7.json"

PROJECT = "bubbo-dfba0"  
LOCATION = "europe-southwest1" 
BUCKET_NAME = "embeddings_new_bucket"  
GCS_PREFIX = "embeddings/movies_and_series"  
BQ_QUERY = "SELECT tmdb_id AS ID, title, genres, synopsis FROM `bubbo-dfba0.content.best_content_translated_py`"
EMBEDDING_MODEL = "textembedding-gecko-multilingual" 

BATCH_SIZE = 1000 

vertexai.init(project=PROJECT, location=LOCATION)
embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
bq_client = bigquery.Client(project=PROJECT)
storage_client = storage.Client(project=PROJECT)

# 1. Obtener datos de BigQuery (por páginas)
def get_data_from_bigquery():
    print("Ejecutando la consulta en BigQuery...")
    
    job = bq_client.query(BQ_QUERY)
    iterator = job.result(page_size=BATCH_SIZE)  
    pages = iterator.pages
    
    print("Consulta ejecutada, procesando los resultados...")

    all_rows = []

    for page_num, page in enumerate(pages, start=1):
        print(f"Procesando página {page_num}...")
        all_rows.extend(page)

    print("Todas las páginas procesadas, verificando la estructura de los datos...")

    # Extraer las tuplas de los objetos Row
    all_rows = [list(row) for row in all_rows]  # Convertimos cada Row a una lista simple

    print(f"Estructura de all_rows: {all_rows[:5]}")  # Muestra las primeras 5 filas para verificar

    # Convertir todas las filas a un DataFrame de pandas
    columns = ['ID', 'title', 'genres', 'synopsis']  # Definir las columnas esperadas
    df = pd.DataFrame(all_rows, columns=columns)
    print("Columnas en el DataFrame:", df.columns)

    # Mostrar las primeras filas para ver el contenido
    print("Primeros registros del DataFrame:")
    print(df.head())  # Mostrar las primeras filas del DataFrame

    # Obtener los datos de las columnas que nos interesan
    ids = df["ID"].astype(str).tolist()  # Obtener los IDs
    texts = df["title"].astype(str).tolist()  # Obtener los títulos
    
    print(f"Se han obtenido {len(ids)} registros de BigQuery.")
    
    return ids, texts


# 2. Crear embeddings de los textos utilizando Vertex AI
def create_embeddings(texts):
    embeddings = embedding_model.get_embeddings(texts)  # Crear embeddings para los textos
    return [e.values for e in embeddings]  # Devuelve una lista de embeddings

# 3. Subir los embeddings a GCS (Google Cloud Storage)
def upload_to_gcs(local_file_path, gcs_path):
    bucket = storage_client.bucket(BUCKET_NAME)  
    blob = bucket.blob(gcs_path)  
    blob.upload_from_filename(local_file_path)  
    print(f"Subido a GCS: {gcs_path}")

# Función para guardar los embeddings en un archivo JSONL y subir a GCS
def save_embeddings_to_gcs(ids, embeddings, part):
    jsonl_path = f"/tmp/embeddings_part_{part}.jsonl"  
    with open(jsonl_path, "w") as f:
        for i in range(len(ids)):
            item = {"id": ids[i], "embedding": embeddings[i]}  
            f.write(json.dumps(item) + "\n")  
    
    gcs_path = f"{GCS_PREFIX}/embeddings_part_{part}.jsonl"  
    upload_to_gcs(jsonl_path, gcs_path)  
    return f"gs://{BUCKET_NAME}/{gcs_path}"

# 4. Procesar en batches, crear embeddings y subir a GCS
def process_and_upload_batches():
    print("Iniciando el proceso de obtener datos y crear embeddings...")
    
    ids, texts = get_data_from_bigquery()

    if not ids or not texts:
        print("Error al obtener los datos. Terminando el proceso.")
        return ""

    print("Creando embeddings para los títulos...")
    embeddings = create_embeddings(texts)  

    print("Subiendo los embeddings a GCS...")
    gcs_file_uri = save_embeddings_to_gcs(ids, embeddings, part=1)  
    
    print("Embeddings subidos con éxito.")
    return gcs_file_uri

# MAIN
if __name__ == "__main__":
    print("Comenzando el proceso de batch...")
    gcs_file_uri = process_and_upload_batches()  
    print(f"Embeddings subidos a: {gcs_file_uri}")
