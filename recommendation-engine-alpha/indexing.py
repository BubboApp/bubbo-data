from google.cloud import vertex_ai

# Crear el cliente de Matching Engine
client = vertex_ai.MatchingEngineClient()

# Definir el recurso del índice
index = {
    'display_name': 'my-index',
    'metadata': {
        'embedding_size': 128,  # Tamaño del vector de embeddings
        'index_algorithm': 'HNSW',  # Algoritmo de indexación (HNSW o IVF)
    },
    'deployed_indexes': [{
        'id': 'my-deployed-index-id',
        'endpoint': 'projects/my-project-id/locations/us-central1/endpoints/my-endpoint-id',
    }]
}

# Crear el índice
response = client.create_index(
    parent="projects/my-project-id/locations/us-central1",
    index=index
)
