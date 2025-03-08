import firebase_admin
from firebase_admin import credentials, firestore
import vertexai
from vertexai.language_models import TextEmbeddingModel

# Inicializa Firebase Admin SDK
cred = credentials.Certificate('bubbo-dfba0-47e395cdcdc7.json')
firebase_admin.initialize_app(cred)

# Inicializa el cliente de Firebase Firestore
db = firestore.client()

# Inicializa Vertex AI
PROJECT_ID = "bubbo-dfba0"
REGION = "us-central1"  # Región donde está alojado el servicio
MODEL_ID = "text-multilingual-embedding-002"  # El ID del modelo que quieres usar

vertexai.init(project=PROJECT_ID, location=REGION)

# Cargar el modelo de embeddings preentrenado
model = TextEmbeddingModel.from_pretrained(MODEL_ID)

def get_text_embedding(text):
    """Genera el embedding para un texto usando Vertex AI"""
    print(f"Generando embedding para: {text[:50]}...")
    embeddings = model.get_embeddings([text])
    return embeddings[0].values  # Devuelve el vector de embeddings

def process_and_store_embeddings():
    """Lee desde la colección de entrada, genera embeddings y guarda en la colección de salida con el mismo ID"""
    print("Iniciando procesamiento de documentos...")

    # Referencias a las colecciones
    input_collection_ref = db.collection('Data_EN')
    output_collection_ref = db.collection('embeddings')

    # Lee los documentos de la colección de entrada
    docs = input_collection_ref.stream()
    total_docs = 0

    for doc in docs:
        data = doc.to_dict()
        text = f"{data.get('CleanTitle', '')} {data.get('Genre', '')} {data.get('Synopsis', '')}".strip()

        if text:
            # Genera el embedding para el texto
            embedding = get_text_embedding(text)

            # Guarda el resultado en la colección de salida con el mismo ID
            output_collection_ref.document(doc.id).set({
                'original_text': text,
                'embedding': embedding
            })
            total_docs += 1
            print(f"Procesado {total_docs}: {doc.id} -> {text[:30]}...")
    
    print(f"Finalizado. Total de documentos procesados: {total_docs}")

# Llamada para procesar y almacenar los embeddings
process_and_store_embeddings()
