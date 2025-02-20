import pandas as pd
from langdetect import detect , DetectorFactory
import numpy as np
from tqdm import tqdm
import tmdbsimple as tmdb

tqdm.pandas()

data = pd.read_csv(r"C:\Users\Carlo\Desktop\Proyectos\data\bubbo-data\Test\extracted_data.csv")

df = data.copy()

def ponderate_and_deduplicate(df):
    """
    Asigna un puntaje a cada fila, ordena el DataFrame por puntaje,
    elimina duplicados por ID y realiza la operación inplace.
    """

    df['Score'] = 0
    df['Score'] += (df['CleanTitle'] != 'No Title') * 2
    df['Score'] += (df['Genre'] != 'No Genre') * 2
    df['Score'] += (df['Synopsis'] != 'No Synopsis') * 2
    df['Score'] += (df['Directors'] != 'No Directors') * 0.5
    df['Score'] += (df['Cast'] != 'No Cast') * 0.5

    # Ordena el DataFrame por 'Score' de forma descendente (inplace)
    df.sort_values(by='Score', ascending=False, inplace=True)

    # Elimina duplicados por 'ID', conservando el primero (inplace)
    df.drop_duplicates(subset='ID', keep='first', inplace=True)

    return df

ponderate_and_deduplicate(df)

def actualizar_ids_tmdb(df):
    """Actualiza IDs de IMDb a TMDB en un DataFrame, modificando la columna 'ID'."""
    
    tmdb.API_KEY = '7425c75e59119610b20640317310953a'
    tqdm.pandas()  # Habilita barra de progreso
    
    # Filtra solo las filas con IDs no numéricos (posibles IDs de IMDb)
    df['ID_numeric'] = pd.to_numeric(df['ID'], errors='coerce')
    imdb = df[df['ID_numeric'].isna()]
    
    def buscar_id_tmdb(imdb_id):
        try:
            find = tmdb.Find(imdb_id)
            response = find.info(external_source='imdb_id')
            if response['movie_results']:
                return response['movie_results'][0]['id']
            elif response['tv_results']:
                return response['tv_results'][0]['id']
            else:
                return None  # Devuelve None si no se encuentra
        except Exception as e:
            print(f"Error para {imdb_id}: {e}")
            return None  # Devuelve None en caso de error
    
    df.loc[imdb.index, 'ID'] = imdb['ID'].progress_apply(buscar_id_tmdb)  
    
    # Elimina filas donde no se encontró un ID válido
    df = df.dropna(subset=['ID'])
    
    df.drop(columns='ID_numeric', inplace=True)
    return df

actualizar_ids_tmdb(df)

def completar_datos_tmdb(df):
    """
    Completa datos faltantes en un DataFrame usando la API de TMDB.
    Intenta completar datos varias veces solo para las filas con datos faltantes.

    Args:
        df: El DataFrame con datos de películas/series.

    Returns:
        El DataFrame con los datos completados.
    """
    api_key = '7425c75e59119610b20640317310953a' 
    tmdb.API_KEY = api_key

    def buscar_y_completar(row):
        """Función interna para buscar y completar datos de una fila."""
        id_tmdb = row['ID']

        # Verifica si la fila ya tiene la información completa
        if not (pd.isna(row['CleanTitle']) or row['CleanTitle'] == 'No Title' or \
                pd.isna(row['Genre']) or row['Genre'] == 'No Genre' or \
                pd.isna(row['Synopsis']) or row['Synopsis'] == 'No Synopsis' or \
                pd.isna(row['Cast']) or row['Cast'] == 'No Cast' or \
                pd.isna(row['Directors']) or row['Directors'] == 'No Directors'):
            return row  # Si ya tiene la información, no hace nada

        try:
            details = tmdb.Movies(id_tmdb).info()
            if not details:
                details = tmdb.TV(id_tmdb).info()

            if not details:
                find = tmdb.Find(id_tmdb)
                response = find.info(external_source='imdb_id')
                if response['movie_results']:
                    id_tmdb = response['movie_results'][0]['id']
                    details = tmdb.Movies(id_tmdb).info()
                elif response['tv_results']:
                    id_tmdb = response['tv_results'][0]['id']
                    details = tmdb.TV(id_tmdb).info()

            if details:
                if pd.isna(row['CleanTitle']) or row['CleanTitle'] == 'No Title':
                    row['CleanTitle'] = details['title'] if 'title' in details else details['name']
                if pd.isna(row['Genre']) or row['Genre'] == 'No Genre':
                    row['Genre'] = ', '.join([genre['name'] for genre in details.get('genres', [])])
                if pd.isna(row['Synopsis']) or row['Synopsis'] == 'No Synopsis':
                    row['Synopsis'] = details.get('overview', '')
                if pd.isna(row['Cast']) or row['Cast'] == 'No Cast':
                    cast = [member['name'] for member in details.get('credits', {}).get('cast', [])]
                    row['Cast'] = ', '.join(cast[:5])
                if pd.isna(row['Directors']) or row['Directors'] == 'No Directors':
                    directors = [member['name'] for member in details.get('credits', {}).get('crew', []) if member['job'] == 'Director']
                    row['Directors'] = ', '.join(directors)

            return row

        except Exception as e:
            print(f"Error al buscar o completar datos para ID {id_tmdb}: {e}")
            return row

    tqdm.pandas()
    max_attempts = 3
    for _ in range(max_attempts):
        df_faltantes = df[
            (df['CleanTitle'] == 'No Title') | 
            (df['Genre'] == 'No Genre') | 
            (df['Synopsis'] == 'No Synopsis') | 
            (df['Cast'] == 'No Cast') | 
            (df['Directors'] == 'No Directors')
        ]
        df_faltantes = df_faltantes.progress_apply(buscar_y_completar, axis=1)
        df.update(df_faltantes)

        df['Score'] = 0
        df['Score'] += (df['CleanTitle'] != 'No Title') * 2
        df['Score'] += (df['Genre'] != 'No Genre') * 2
        df['Score'] += (df['Synopsis'] != 'No Synopsis') * 2
        df['Score'] += (df['Directors'] != 'No Directors') * 0.5
        df['Score'] += (df['Cast'] != 'No Cast') * 0.5
        print(df['Score'].value_counts())

    df = df[df['Score'] >= 6]

    return df

completar_datos_tmdb(df)





