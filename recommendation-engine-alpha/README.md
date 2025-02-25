| **Módulo/Función**     | **Responsable**  | **Descripción**              | **Inputs**              | **Outputs**              |
|------------------------|-----------------|-----------------------------|-------------------------|--------------------------|
| `Clean_data.py`         | Carlos Mijares      | Carga y limpieza de datos   | Archivos CSV      | DataFrame limpio        |
| `DownloadFirebase.py` |   Carlos Mijares     | Descarga de la Base de datos |  -------            |  DataFrame Bruto   |
| `uploaded:json.py`       |  Carlos Mijares     |  Carga de JSON a DB en Firestore     |  Archivo JSON  |  --------      |
| `from_data_to_embedds.ipynb`    |  Agustin Lujan     | Obtiene datos desde Firebase: 'Movies & Series content' y 'Genres'. Obtiene informacion de 'User Preferences content' y prepara la info para la obtencion de embeddings. Obtiene los embeddings | 'Data_Clean', 'Genres_DB', DynamoDB 'user_pref...'  | Embeddings to ¿VERTEX?  |
| `translations.ipynb`       |  Agustin Lujan     |  con funcion translate_df(), envia DFs a la Api de google     |  271k de contenidos  |  devuelve el DF con columnas 'translated'  |
