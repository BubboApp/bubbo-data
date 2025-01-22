import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# --- Configuración de Archivos ---
FILTERED_DATA_FILE = "filtered_data_final.csv"
USER_PREFERENCES_FILE = "user_preferences_final.csv"
OUTPUT_RECOMMENDATIONS_FILE = "recommendations.csv"

# --- Cargar Archivos ---
print("Cargando archivos...")
filtered_data = pd.read_csv(FILTERED_DATA_FILE, sep=";")
user_preferences = pd.read_csv(USER_PREFERENCES_FILE, sep=";")

# --- Verificar tipos de datos ---
print("\n=== Verificando tipos de datos ===")
print(f"ExternalIds en filtered_data: {filtered_data['ExternalIds'].dtype}")
print(f"PeliculasFavoritas en user_preferences: {type(user_preferences['PeliculasFavoritas'].iloc[0])}")

# Asegurar que ExternalIds es de tipo string para facilitar comparaciones
filtered_data["ExternalIds"] = filtered_data["ExternalIds"].astype(str)

# --- Preparación de Datos ---
print("\nPreparando datos para TF-IDF...")
# Combinar columnas relevantes en un solo campo de texto
filtered_data["Content"] = (
    filtered_data["CleanTitle"].fillna("") + " " +
    filtered_data["Synopsis"].fillna("") + " " +
    filtered_data["Genres"].fillna("")
)

# Verificar las primeras filas de la columna Content
print("\nPrimeras filas de la columna 'Content':")
print(filtered_data["Content"].head())

# --- Vectorización TF-IDF ---
print("\nVectorizando contenido con TF-IDF...")
tfidf_vectorizer = TfidfVectorizer(stop_words="english")  # Cambiamos a 'english' porque 'spanish' no es válido
tfidf_matrix = tfidf_vectorizer.fit_transform(filtered_data["Content"])

# Verificar la forma de la matriz TF-IDF
print(f"Matriz TF-IDF: {tfidf_matrix.shape}")

# --- Función para Crear Recomendaciones ---
def recomendar_contenido(favoritos_ids, filtered_data, tfidf_matrix, top_n=10):
    """
    Recomienda contenido basado en los IDs favoritos y una matriz TF-IDF.
    """
    # Filtrar solo los índices de los contenidos favoritos
    favoritos_indices = filtered_data[filtered_data["ExternalIds"].isin(favoritos_ids)].index.tolist()

    if not favoritos_indices:
        return []  # Si no hay favoritos encontrados, devolver vacío

    # Calcular similitud del coseno entre los favoritos y todos los contenidos
    favoritos_tfidf = tfidf_matrix[favoritos_indices]
    similitudes = cosine_similarity(favoritos_tfidf, tfidf_matrix)

    # Promediar las similitudes para obtener un puntaje general por contenido
    similitud_promedio = np.mean(similitudes, axis=0)

    # Obtener los índices de los contenidos más similares
    top_indices = similitud_promedio.argsort()[-top_n - len(favoritos_ids):][::-1]

    # Excluir los IDs favoritos del resultado final
    recomendaciones = [
        idx for idx in top_indices if filtered_data.iloc[idx]["ExternalIds"] not in favoritos_ids
    ]

    # Devolver los títulos de los contenidos recomendados
    return filtered_data.iloc[recomendaciones][["ExternalIds", "CleanTitle", "Genres", "Synopsis"]].head(top_n)

# --- Depuración de Recomendaciones ---
def depurar_recomendaciones(user_preferences, filtered_data, tfidf_matrix):
    """
    Depuración detallada para analizar por qué no se generan recomendaciones.
    """
    for index, row in user_preferences.iterrows():
        user_id = row["userId"]
        peliculas_favoritas = row["PeliculasFavoritas"]
        series_favoritas = row["SeriesFavoritas"]
        favoritos_ids = eval(peliculas_favoritas) + eval(series_favoritas)

        print(f"\nUsuario: {user_id}")
        print(f"Películas y series favoritas: {favoritos_ids}")

        # Verificar si los IDs favoritos existen en filtered_data
        favoritos_indices = filtered_data[filtered_data["ExternalIds"].isin(favoritos_ids)].index.tolist()
        print(f"Índices encontrados en filtered_data: {favoritos_indices}")

        if favoritos_indices:
            favoritos_tfidf = tfidf_matrix[favoritos_indices]
            similitudes = cosine_similarity(favoritos_tfidf, tfidf_matrix)
            print(f"Similitudes calculadas para {user_id}: {similitudes.shape}")

            # Si se calculan similitudes, mostrar los contenidos recomendados
            similitud_promedio = np.mean(similitudes, axis=0)
            top_indices = similitud_promedio.argsort()[-10 - len(favoritos_ids):][::-1]
            recomendaciones = [
                idx for idx in top_indices if filtered_data.iloc[idx]["ExternalIds"] not in favoritos_ids
            ]
            print(f"Recomendaciones generadas: {len(recomendaciones)}")
        else:
            print(f"No se encontraron favoritos en filtered_data para {user_id}.")

# --- Generar Recomendaciones ---
print("\nGenerando recomendaciones...")
recomendaciones_usuarios = []

for _, row in user_preferences.iterrows():
    user_id = row["userId"]
    peliculas_favoritas = eval(row["PeliculasFavoritas"])
    series_favoritas = eval(row["SeriesFavoritas"])

    # Unificar películas y series favoritas
    favoritos_ids = peliculas_favoritas + series_favoritas

    # Generar recomendaciones
    recomendaciones = recomendar_contenido(favoritos_ids, filtered_data, tfidf_matrix, top_n=10)

    # Agregar a la lista de resultados
    for _, rec in recomendaciones.iterrows():
        recomendaciones_usuarios.append({
            "userId": user_id,
            "ExternalId": rec["ExternalIds"],
            "Title": rec["CleanTitle"],
            "Genres": rec["Genres"],
            "Synopsis": rec["Synopsis"]
        })

# --- Guardar Recomendaciones ---
if recomendaciones_usuarios:
    print("\nGuardando recomendaciones...")
    recomendaciones_df = pd.DataFrame(recomendaciones_usuarios)
    recomendaciones_df.to_csv(OUTPUT_RECOMMENDATIONS_FILE, sep=";", index=False)
    print(f"Recomendaciones guardadas en: {OUTPUT_RECOMMENDATIONS_FILE}")
else:
    print("\nNo se generaron recomendaciones. Ejecutando depuración...")
    depurar_recomendaciones(user_preferences, filtered_data, tfidf_matrix)

print("\n=== PROCESO COMPLETADO ===")
