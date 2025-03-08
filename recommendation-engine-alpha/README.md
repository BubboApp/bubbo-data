| **Módulo/Función**     | **Responsable**  | **Descripción**              | **Inputs**              | **Outputs**              |
|------------------------|-----------------|-----------------------------|-------------------------|--------------------------|
| `Clean_data.py`         | Carlos Mijares      | Carga y limpieza de datos   | Archivos CSV      | DataFrame limpio        |
| `DownloadFirebase.py` |   Carlos Mijares     | Descarga de la Base de datos |  -------            |  DataFrame Bruto   |
| `uploaded:json.py`       |  Carlos Mijares     |  Carga de JSON a DB en Firestore     |  Archivo JSON  |  --------      |
| `from_user_pref_to_similarities.ipynb`    |  Agustin Lujan     | Obtiene datos desde Dynamodb de 'User Preferences content' y prepara la info para la obtencion de embeddings. Obtiene los embeddings, y genera recomendaciones por similaridad con las pref del usuario | 'Data_EN', DynamoDB 'user_pref...', embeddings_bucket_backup  | Recomendaciones en tiempo real  |
| `translations.ipynb`       |  Agustin Lujan     |  con funcion translate_df(), envia DFs a la Api de google     |  271k de contenidos  |  devuelve el DF con columnas 'translated'  |
| `embeddings_generator.py`       |  Ruben Carrasco     |  Genera el embedding para un texto usando Vertex AI, con el modelo embedding-002 para generar multi lenguajes     |  collection Data_EN  |  collection embeddings  |
