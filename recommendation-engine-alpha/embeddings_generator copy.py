import firebase_admin
from firebase_admin import credentials, firestore
import vertexai
from vertexai.language_models import TextEmbeddingModel
from google.cloud import storage
import json

# Inicialización
cred = credentials.Certificate('bubbo-dfba0-47e395cdcdc7.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

PROJECT_ID = "bubbo-dfba0"
REGION = "us-central1"
MODEL_ID = "text-multilingual-embedding-002"
vertexai.init(project=PROJECT_ID, location=REGION)
model = TextEmbeddingModel.from_pretrained(MODEL_ID)

# Cloud Storage
storage_client = storage.Client()
bucket_name = "all_content_embeddings_for_matching"
bucket = storage_client.bucket(bucket_name)

def get_text_embedding(text):
    try:
        print(f"Generando embedding para: {text[:50]}...")
        embeddings = model.get_embeddings([text])
        return embeddings[0].values
    except Exception as e:
        print(f"Error al generar embedding para {text[:50]}: {e}")
        return None

def process_and_store_embeddings():
    print("Iniciando procesamiento de documentos...")
    input_collection_ref = db.collection('Data_EN')
    docs = input_collection_ref.stream()
    total_docs = 0
    updated_docs = 0
    failed_embeddings = []

    # Cargar IDs existentes del bucket
    existing_ids = set()
    for blob in bucket.list_blobs():
        if blob.name.endswith('.json'):
            try:
                content = blob.download_as_text()
                existing_data = json.loads(content)
                if isinstance(existing_data, list):
                    for item in existing_data:
                        existing_ids.add(item.get('ID'))
                elif isinstance(existing_data, dict):
                    existing_ids.add(existing_data.get('ID'))
            except json.JSONDecodeError:
                print(f"Error al decodificar JSON del blob: {blob.name}")
            except Exception as e:
                print(f"Error al procesar blob {blob.name}: {e}")

    print(f"Se encontraron {len(existing_ids)} IDs existentes en el bucket.")

    for doc in docs:
        try:
            data = doc.to_dict()
            text = f"{data.get('CleanTitle', '')} {data.get('Genre', '')} {data.get('Synopsis', '')}".strip()

            if text:
                embedding = get_text_embedding(text)
                if embedding is None:
                    print(f"Error al obtener embedding para {doc.id}. Se omite.")
                    failed_embeddings.append(doc.id)
                    continue

                embedding_data = {
                    'ID': doc.id,
                    'original_text': text,
                    'embedding': embedding
                }
                json_data = json.dumps(embedding_data).encode('utf-8')
                blob_name = f"{doc.id}.json"

                if doc.id not in existing_ids:
                    blob = bucket.blob(blob_name)
                    blob.upload_from_string(json_data, content_type='application/json')
                    total_docs += 1
                    updated_docs += 1
                    print(f"Procesado {total_docs}: {doc.id} (Nuevo) -> {text[:30]}...")

                elif doc.id in existing_ids:
                    blob = bucket.blob(blob_name)
                    blob.upload_from_string(json_data, content_type='application/json')
                    total_docs += 1
                    print(f"Procesado {total_docs}: {doc.id} (Actualizado) -> {text[:30]}...")

                if total_docs % 1000 == 0:
                    print(f"Progreso: {total_docs} documentos procesados.")

        except Exception as e:
            print(f"Error al procesar documento {doc.id}: {e}")
            failed_embeddings.append(doc.id)

    # Guardar los IDs de los embeddings fallidos
    if failed_embeddings:
        with open("failed_embeddings.json", "w") as f:
            json.dump(failed_embeddings, f, indent=4)
        print(f"Se encontraron {len(failed_embeddings)} embeddings fallidos. IDs guardados en failed_embeddings.json")

    print(f"Finalizado. Total de documentos procesados: {total_docs}. Documentos Nuevos o actualizados: {updated_docs}")

process_and_store_embeddings()