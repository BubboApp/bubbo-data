import os
import json
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import storage, bigquery, aiplatform
from vertexai.language_models import TextEmbeddingModel
from google.cloud.aiplatform.matching_engine.matching_engine_index_endpoint import MatchingEngineIndexEndpoint

# ====== CONFIGURACIÓN ======
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bubbo-dfba0-47e395cdcdc7.json"

PROJECT = "bubbo-dfba0"
LOCATION = "europe-southwest1"
BUCKET = "gs://embeddings_new_bucket"
INDEX_NAME = "indices_netflix2"
ENDPOINT_ID = "projects/75629471929/locations/europe-southwest1/indexEndpoints/5785049643217846272"
MODEL_ID = "text-multilingual-embedding-002"

# ====== INIT VERTEX AI ======
from vertexai import init as vertexai_init
vertexai_init(project=PROJECT, location=LOCATION)

# ====== FUNCIONES AUXILIARES ======
def extract_tmdb_id(external_ids):
    if not external_ids or not isinstance(external_ids, list):
        return None
    for ext_id in external_ids:
        if ext_id and ext_id.get("Provider") == "tmdb":
            return ext_id.get("ID")
    return None

def create_master_embeddings():
    model = TextEmbeddingModel.from_pretrained(MODEL_ID)
    all_movies = []
    processed, skipped = 0, 0

    with open("es_netflix.jsonl", "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f):
            try:
                movie = json.loads(line)
                synopsis = movie.get("Synopsis", "")
                external_ids = movie.get("ExternalIds")
                if not synopsis:
                    skipped += 1
                    continue
                tmdb_id = extract_tmdb_id(external_ids)
                if tmdb_id:
                    all_movies.append({
                        "tmdb_id": tmdb_id,
                        "title": movie.get("Title", "Sin título"),
                        "synopsis": synopsis,
                        "genres": movie.get("Genres", []),
                        "year": movie.get("Year"),
                        "type": movie.get("Type", "")
                    })
                    processed += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
                continue

    print(f"✅ Procesadas: {processed}, ⚠️ Omitidas: {skipped}")
    if not all_movies:
        return []

    batch_size = 20
    for i in tqdm(range(0, len(all_movies), batch_size), desc="Embeddings"):
        batch = all_movies[i:i + batch_size]
        synopses = [movie["synopsis"] for movie in batch]
        try:
            embeddings = model.get_embeddings(synopses)
            for j, movie in enumerate(batch):
                movie["embedding"] = embeddings[j].values
        except Exception as e:
            print(f"❌ Error en lote {i // batch_size + 1}: {e}")
            continue

    return [{"tmdb_id": m["tmdb_id"], "embedding": m["embedding"]} for m in all_movies]

def build_embedding_dict(embedding_list):
    return {item["tmdb_id"]: item["embedding"] for item in embedding_list if item.get("tmdb_id") and item.get("embedding")}

def recommend(movie_tmdb_id, movie_embedding):
    index_endpoint = MatchingEngineIndexEndpoint(
        index_endpoint_name=ENDPOINT_ID,
        project=PROJECT,
        location=LOCATION
    )

    response = index_endpoint.find_neighbors(
        deployed_index_id="netflix_movies_index",
        queries=[movie_embedding],
        num_neighbors=11
    )

    return [neighbor.id for neighbor in response[0] if neighbor.id != movie_tmdb_id]

def mostrar_recomendaciones(recommended_ids, path="es_netflix.jsonl"):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            movie = json.loads(line)
            external_ids = movie.get("ExternalIds", [])
            tmdb_id = extract_tmdb_id(external_ids)
            if tmdb_id in recommended_ids:
                print(f"🎞️ {movie.get('Title', 'Sin título')} (TMDB ID: {tmdb_id})")
                print(f"   Netflix ID: {movie.get('Id', 'N/A')}")
                print(f"   Géneros: {movie.get('Genres', [])}")
                print("---")

# ========== MAIN ==========
if __name__ == "__main__":
    print("🎬 Creando embeddings maestro...")
    master_embeddings = create_master_embeddings()
    emb_dict = build_embedding_dict(master_embeddings)

    movie_tmdb_id = "475557"  # <- Cambia esto si quieres otro
    movie_embedding = emb_dict.get(movie_tmdb_id)

    if movie_embedding:
        print("🔍 Consultando recomendaciones...")
        recommended_ids = recommend(movie_tmdb_id, movie_embedding)
        print(f"📌 IDs recomendados: {recommended_ids}")
        mostrar_recomendaciones(recommended_ids)
    else:
        print(f"❌ No se encontró embedding para TMDB ID: {movie_tmdb_id}")
