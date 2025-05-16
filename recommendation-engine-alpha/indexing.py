from google.cloud import aiplatform
import time

# Inicializar cliente de Vertex AI
aiplatform.init(
    project="bubbo-dfba0",
    location="us-central1",
    staging_bucket="gs://embeddings_new_bucket"
)

# Crear y desplegar el índice
index = aiplatform.MatchingEngineIndex.create(
    display_name="index-for-alpha-recs-movies-and-tv-shows-hnsw-uscentral1",
    contents_delta_uri="gs://embeddings_new_bucket/embeddings/index_data/all_embeddings.jsonl",
    index_update_method="STREAM_UPDATE",
    embedding_dimension=768,
    distance_measure_type="COSINE_DISTANCE",
    approximate_neighbors_count=250,
    deployed_indexes=[
        {
            "id": "recommendation-engine-001",
            "endpoint": "projects/75629471929/locations/us-central1/indexEndpoints/6508829560480464896",
        }
    ],
    machine_type="e2-standard-4",
)

# Monitorear progreso
print("Iniciando creación de índice...")

operation = index._gca_resource.create_time  # Marca de creación (informativo)

# Polling manual de estado
while not index.done():
    print("Estado: Creando índice... (esperando 30 segundos)")
    time.sleep(30)
    index.reload()  # Refrescar estado desde Vertex AI

if index.error_code:
    print(f"Error al crear el índice: {index.error_message}")
else:
    print(f"Índice creado exitosamente: {index.resource_name}")
