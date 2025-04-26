from google.cloud import vertex_ai

# Crear el cliente de Matching Engine
client = vertex_ai.MatchingEngineClient()

# Definir el recurso del índice
index = {
    'display_name': 'index_for_alpha-recs-movies_and_tv_shows-hnsw-uscentral1',
    'metadata': {
        'embedding_size': 768,  # Tamaño del vector de embeddings
        'index_algorithm': 'HNSW',  # Algoritmo de indexación (HNSW o IVF)
    },
    'deployed_indexes': [{
        'id': 'recommendation-engine-001',
        'endpoint': 'projects/75629471929/locations/us-central1/indexEndpoints/6508829560480464896',
    }]
}

# Crear el índice
response = client.create_index(
    parent="projects/bubbo-dfba0/locations/us-central1",
    index=index
)
