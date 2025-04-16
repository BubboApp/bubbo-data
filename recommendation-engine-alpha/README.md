| **Módulo/Función**     | **Responsable**  | **Descripción**              | **Inputs**              | **Outputs**              |
|------------------------|-----------------|-----------------------------|-------------------------|--------------------------|
| embeddings_generator.py | Agustin Lujan | Une y normaliza múltiples tablas del dataset bbmedia de BigQuery en una tabla unificada (unified_content) y luego genera una tabla optimizada (best_content) con los registros más completos por tmdb_id. | bubbo-dfba0.content.best_content_translated_py | PROJECT = "bubbo-dfba0"
LOCATION = "europe-southwest1"
BUCKET_NAME = "embeddings_new_bucket" | PROJECT = "bubbo-dfba0"
GCS_PREFIX = "embeddings/movies_and_series" |
| `from_user_pref_to_similarities.ipynb`    |  Agustin Lujan     | Obtiene datos desde Dynamodb de 'User Preferences content' y prepara la info para la obtencion de embeddings. Obtiene los embeddings, y genera recomendaciones por similaridad con las pref del usuario | 'Data_EN', DynamoDB 'user_pref...', embeddings_bucket_backup  | Recomendaciones en tiempo real  |
| `translations.ipynb`       |  Agustin Lujan     |  con funcion translate_df(), envia DFs a la Api de google     |  271k de contenidos  |  devuelve el DF con columnas 'translated'  |
| `embeddings_generator.py`       |  Ruben Carrasco     |  Genera el embedding para un texto usando Vertex AI, con el modelo embedding-002 para generar multi lenguajes     |  collection Data_EN  |  collection embeddings  |
