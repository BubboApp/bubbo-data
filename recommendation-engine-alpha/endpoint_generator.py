from google.cloud import aiplatform_v1

PROJECT  = "bubbo-dfba0"
LOCATION = "us-central1"  # región soportada por Matching Engine

# 1) Cliente apuntando al endpoint regional
client = aiplatform_v1.IndexEndpointServiceClient(
    client_options={
        "api_endpoint": f"{LOCATION}-aiplatform.googleapis.com"
    }
)

# 2) Parent con la región correcta
parent = f"projects/{PROJECT}/locations/{LOCATION}"

# 3) Nombre descriptivo para tu endpoint alpha
index_endpoint = aiplatform_v1.IndexEndpoint(
    display_name="alpha-recs-movies_and_tv_shows-hnsw-uscentral1"
)

# 4) Verificar si ya existe
existing = client.list_index_endpoints(parent=parent)
if any(ep.display_name == index_endpoint.display_name for ep in existing):
    print("Ya existe un endpoint con ese nombre. Aborta o cambia el nombre.")
else:
    # 5) Crear
    op = client.create_index_endpoint(
        parent=parent,
        index_endpoint=index_endpoint
    )
    print("Creando endpoint…")
    ep = op.result()
    print(f"Endpoint creado: {ep.name}")
