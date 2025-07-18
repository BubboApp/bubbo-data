from recommendation_model import recommend, build_embedding_dict, create_master_embeddings

def get_recommendations_from_last_seen(tmdb_id):
    master_embeddings = create_master_embeddings()
    emb_dict = build_embedding_dict(master_embeddings)
    embedding = emb_dict.get(tmdb_id)
    
    if embedding:
        return recommend(tmdb_id, embedding)
    else:
        return []
