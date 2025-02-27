import pandas as pd
import numpy as np
from tqdm import tqdm
import tmdbsimple as tmdb


tqdm.pandas()
print("Cargando data")
data = pd.read_csv(r"C:\Users\Carlo\Desktop\Proyectos\data\bubbo-data\Test\extracted_data.csv")

df = data.copy()

if df[['Genre', 'CleanTitle', 'Synopsis', 'Directors', 'Cast']].isna().any().any():
    cols_a_rellenar = ['Genre', 'CleanTitle', 'Synopsis', 'Directors', 'Cast']
    df[cols_a_rellenar] = df[cols_a_rellenar].fillna({col: f'No {col}' for col in cols_a_rellenar})


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
print("Ponderando y eliminando duplicados")
ponderate_and_deduplicate(df)
print("Actualizando IDs de IMDb a TMDB")
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

# Configuración de API de TMDb
tmdb.API_KEY = '7425c75e59119610b20640317310953a'

def buscar_y_completar(row):
    """Busca y completa datos faltantes de una fila en TMDb."""
    id_tmdb = row['ID']

    # Si la fila ya tiene datos completos, no hace nada
    if all(row[col] != f'No {col}' for col in ['CleanTitle', 'Genre', 'Synopsis', 'Cast', 'Directors']):
        return row

    try:
        # Obtener detalles de la película/serie
        details = tmdb.Movies(id_tmdb).info() or tmdb.TV(id_tmdb).info()

        # Si no encuentra detalles, intentar con IMDb
        if not details:
            response = tmdb.Find(id_tmdb).info(external_source='imdb_id')
            id_tmdb = response.get('movie_results', [{}])[0].get('id') or response.get('tv_results', [{}])[0].get('id')
            details = tmdb.Movies(id_tmdb).info() if id_tmdb else {}

        # Actualizar solo los valores faltantes
        row['CleanTitle'] = details.get('title', details.get('name', '')) if row['CleanTitle'] == 'No Title' else row['CleanTitle']
        row['Genre'] = ', '.join(g['name'] for g in details.get('genres', [])) if row['Genre'] == 'No Genre' else row['Genre']
        row['Synopsis'] = details.get('overview', '') if row['Synopsis'] == 'No Synopsis' else row['Synopsis']
        row['Cast'] = ', '.join(m['name'] for m in details.get('credits', {}).get('cast', [])[:5]) if row['Cast'] == 'No Cast' else row['Cast']
        row['Directors'] = ', '.join(
            m['name'] for m in details.get('credits', {}).get('crew', []) if m['job'] == 'Director'
        ) if row['Directors'] == 'No Directors' else row['Directors']

    except Exception as e:
        print(f"⚠️ Error en ID {id_tmdb}: {e}")

    return row

print('completar_datos_tmdb')
def completar_datos_tmdb(df, chunk_size=100_000, output_file="datos_actualizados.csv"):
    """
    Completa los datos de TMDb procesando el DataFrame en bloques para reducir el consumo de RAM.
    
    Args:
        df (DataFrame): El DataFrame con datos de películas/series.
        chunk_size (int): Tamaño de los bloques a procesar en memoria.
        output_file (str): Nombre del archivo donde se guardará el resultado.
    
    Returns:
        DataFrame con los datos completados.
    """
    tqdm.pandas()  # Habilita barra de progreso en Pandas

    # Filtrar solo las filas con datos faltantes
    mask = df[['CleanTitle', 'Genre', 'Synopsis', 'Cast', 'Directors']].isin(
        ['No Title', 'No Genre', 'No Synopsis', 'No Cast', 'No Directors']
    ).any(axis=1)
    df_faltantes = df[mask]

    # Procesar en bloques para evitar sobrecarga de memoria
    for start in tqdm(range(0, len(df_faltantes), chunk_size), desc="Procesando bloques"):
        end = start + chunk_size
        df_chunk = df_faltantes.iloc[start:end].progress_apply(buscar_y_completar, axis=1)

        # Guardar cada bloque en disco para evitar consumir toda la RAM
        df_chunk.to_csv(f'tmdb_chunk_{start}.csv', index=False)

        # Opcional: ir actualizando el DataFrame principal
        df.update(df_chunk)

    # Calcular Score en memoria sin afectar todo el DataFrame
    puntuaciones = {'CleanTitle': 2, 'Genre': 2, 'Synopsis': 2, 'Directors': 0.5, 'Cast': 0.5}
    df['Score'] = sum((df[col] != f'No {col}') * pts for col, pts in puntuaciones.items())

    print(df['Score'].value_counts())

    # Filtrar solo los datos completos
    df_final = df[df['Score'] >= 6]
    
    # Guardar el resultado final
    df_final.to_csv(output_file, index=False)
    
    return df_final

# Ejecutar la función optimizada
df_actualizado = completar_datos_tmdb(df)

# Guardar el DataFrame final
df_actualizado.to_csv('datos_completos.csv', index=False)







