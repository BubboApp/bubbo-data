import requests
import pandas as pd
import time
import json

# Tu API Key de TMDB
TMDB_API_KEY = "205c53d329bdf08ffe3ebd5acf71005e"

# URLs base de la API
BASE_URL_MOVIE = "https://api.themoviedb.org/3/movie/"
BASE_URL_TV = "https://api.themoviedb.org/3/tv/"

# Cargar los archivos existentes
filtered_data = pd.read_csv("filtered_data_cleaned.csv") #Poner user_preferences_final
user_preferences = pd.read_csv("user_preferences_enriched_cleaned.csv", sep=";") #Poner el filtered_data mas actualizado

# Asegurarse de que los IDs sean listas reales
user_preferences["PeliculasFavoritas"] = user_preferences["PeliculasFavoritas"].apply(eval)
user_preferences["SeriesFavoritas"] = user_preferences["SeriesFavoritas"].apply(eval)

# Identificar IDs faltantes
print("Identificando IDs faltantes...")
ids_peliculas = set(id_ for lista_ids in user_preferences["PeliculasFavoritas"] for id_ in lista_ids)
ids_series = set(id_ for lista_ids in user_preferences["SeriesFavoritas"] for id_ in lista_ids)
ids_en_filtered = set(filtered_data["ExternalIds"].astype(str))  # Convertir a string para comparar

movies_not_found = ids_peliculas - ids_en_filtered
series_not_found = ids_series - ids_en_filtered

print(f"Películas faltantes: {len(movies_not_found)}")
print(f"Series faltantes: {len(series_not_found)}")

# Función para obtener datos de una película o serie desde TMDB
def get_tmdb_data(tmdb_id, is_movie=True):
    """
    Llama a la API de TMDB para obtener detalles de una película o serie.
    :param tmdb_id: ID de TMDB
    :param is_movie: True si es película, False si es serie
    :return: Diccionario con los datos obtenidos o None si falla
    """
    url = BASE_URL_MOVIE if is_movie else BASE_URL_TV
    url += str(tmdb_id)
    params = {"api_key": TMDB_API_KEY, "language": "es"}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code} al obtener el ID {tmdb_id}: {response.text}")
            return None
    except Exception as e:
        print(f"Error al conectar con TMDB para el ID {tmdb_id}: {e}")
        return None

# Crear un nuevo DataFrame para guardar los datos enriquecidos
new_data = []
errores = []

# Enriquecer datos de películas
print("Enriqueciendo datos de películas...")
for movie_id in movies_not_found:
    data = get_tmdb_data(movie_id, is_movie=True)
    if data:
        new_data.append({
            "PlatformName": "TMDB",
            "ExternalIds": movie_id,
            "CleanTitle": data.get("title", ""),
            "Deeplinks": "",
            "Synopsis": data.get("overview", ""),
            "Image": data.get("poster_path", ""),
            "Genres": json.dumps([genre["name"] for genre in data.get("genres", [])]),
            "Cast": "",
            "Crew": "",
            "Directors": ""
        })
    else:
        errores.append(movie_id)
    time.sleep(0.2)  # Respetar límites de la API (5 solicitudes por segundo)

# Enriquecer datos de series
print("Enriqueciendo datos de series...")
for series_id in series_not_found:
    data = get_tmdb_data(series_id, is_movie=False)
    if data:
        new_data.append({
            "PlatformName": "TMDB",
            "ExternalIds": series_id,
            "CleanTitle": data.get("name", ""),
            "Deeplinks": "",
            "Synopsis": data.get("overview", ""),
            "Image": data.get("poster_path", ""),
            "Genres": json.dumps([genre["name"] for genre in data.get("genres", [])]),
            "Cast": "",
            "Crew": "",
            "Directors": ""
        })
    else:
        errores.append(series_id)
    time.sleep(0.2)  # Respetar límites de la API

# Convertir los datos nuevos a un DataFrame
new_data_df = pd.DataFrame(new_data)

# Combinar los nuevos datos con el filtered_data existente
print("Actualizando filtered_data...")
filtered_data_updated = pd.concat([filtered_data, new_data_df]).drop_duplicates(subset="ExternalIds").reset_index(drop=True)

# Guardar el archivo enriquecido
filtered_data_updated.to_csv("filtered_data_actualizado.csv", index=False, encoding="utf-8-sig")
print("Archivo actualizado guardado en: filtered_data_actualizado.csv")

# Guardar errores en un archivo de registro
with open("errores_tmdb.log", "w") as log:
    log.write("\n".join(map(str, errores)))
print("Errores guardados en: errores_tmdb.log")
