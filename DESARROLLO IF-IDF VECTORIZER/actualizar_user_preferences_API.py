import requests
import pandas as pd
import time

# Tu API Key de TMDB
TMDB_API_KEY = "205c53d329bdf08ffe3ebd5acf71005e"

# URLs base de la API
BASE_URL_MOVIE = "https://api.themoviedb.org/3/movie/"
BASE_URL_TV = "https://api.themoviedb.org/3/tv/"

# Cargar el archivo de user_preferences
user_preferences = pd.read_csv("user_preferences_cleaned.csv", sep=",") #Poner user_preferences mas actual.

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

# Enriquecer películas favoritas
print("Enriqueciendo datos de películas favoritas...")
enriched_movies = []
for index, row in user_preferences.iterrows():
    movie_ids = str(row["PeliculasFavoritas"]).split(";") if pd.notna(row["PeliculasFavoritas"]) else []
    movie_details = []
    for movie_id in movie_ids:
        if movie_id.isdigit():  # Verificar que el ID sea numérico
            data = get_tmdb_data(movie_id, is_movie=True)
            if data:
                movie_details.append({
                    "Titulo": data.get("title", ""),
                    "Sinopsis": data.get("overview", ""),
                    "FechaEstreno": data.get("release_date", ""),
                    "Generos": [genre["name"] for genre in data.get("genres", [])],
                    "Popularidad": data.get("popularity", 0),
                    "PuntajePromedio": data.get("vote_average", 0),
                })
            time.sleep(0.2)  # Respetar límites de la API
    enriched_movies.append(movie_details)

# Guardar los datos enriquecidos de películas en una nueva columna
user_preferences["DetallesPeliculasEnriquecido"] = enriched_movies

# Enriquecer series favoritas
print("Enriqueciendo datos de series favoritas...")
enriched_series = []
for index, row in user_preferences.iterrows():
    series_ids = str(row["SeriesFavoritas"]).split(";") if pd.notna(row["SeriesFavoritas"]) else []
    series_details = []
    for series_id in series_ids:
        if series_id.isdigit():  # Verificar que el ID sea numérico
            data = get_tmdb_data(series_id, is_movie=False)
            if data:
                series_details.append({
                    "Titulo": data.get("name", ""),
                    "Sinopsis": data.get("overview", ""),
                    "FechaEstreno": data.get("first_air_date", ""),
                    "Generos": [genre["name"] for genre in data.get("genres", [])],
                    "Popularidad": data.get("popularity", 0),
                    "PuntajePromedio": data.get("vote_average", 0),
                })
            time.sleep(0.2)  # Respetar límites de la API
    enriched_series.append(series_details)

# Guardar los datos enriquecidos de series en una nueva columna
user_preferences["DetallesSeriesEnriquecido"] = enriched_series

# Guardar el archivo enriquecido
user_preferences.to_csv("user_preferences_enriched.csv", sep=";", index=False, encoding="utf-8-sig")
print("Archivo enriquecido guardado en: user_preferences_enriched.csv") #Genera user_preference actualizado desde la API
