import pandas as pd

# --- Configuración de Archivos ---
RECOMMENDATIONS_FILE = "recommendations.csv"
USER_PREFERENCES_FILE = "user_preferences_final.csv"
OUTPUT_ANALYSIS_FILE = "recommendations_analysis.csv"

# --- Cargar Archivos ---
print("Cargando archivos...")
recommendations = pd.read_csv(RECOMMENDATIONS_FILE, sep=";")
user_preferences = pd.read_csv(USER_PREFERENCES_FILE, sep=";")

# --- Procesar y Analizar ---
print("\nAnalizando la calidad de las recomendaciones...")

# Expandir las columnas de géneros y convertir a listas
recommendations["Genres"] = recommendations["Genres"].apply(eval)
user_preferences["GenerosFavoritos"] = user_preferences["GenerosFavoritos"].apply(eval)

# Crear columnas para analizar coincidencias de géneros y sinopsis
def analizar_coincidencia_generos(user_id, recomendacion_generos):
    """Comprueba si hay coincidencia de géneros entre la recomendación y los géneros favoritos del usuario."""
    favoritos = user_preferences.loc[user_preferences["userId"] == user_id, "GenerosFavoritos"].iloc[0]
    return any(genero in favoritos for genero in recomendacion_generos)

def analizar_coincidencia_sinopsis(user_id, recomendacion_sinopsis):
    """Comprueba si las palabras clave de la sinopsis de las preferencias del usuario coinciden con la recomendación."""
    peliculas_sinopsis = user_preferences.loc[user_preferences["userId"] == user_id, "DetallesPeliculas"].iloc[0]
    series_sinopsis = user_preferences.loc[user_preferences["userId"] == user_id, "DetallesSeries"].iloc[0]
    # Concatenar todas las sinopsis favoritas
    favoritas_sinopsis = f"{peliculas_sinopsis} {series_sinopsis}"
    return any(palabra in favoritas_sinopsis.lower() for palabra in recomendacion_sinopsis.lower().split())

# Analizar cada recomendación
recommendations["GenerosCoinciden"] = recommendations.apply(
    lambda row: analizar_coincidencia_generos(row["userId"], row["Genres"]),
    axis=1
)

recommendations["SinopsisCoinciden"] = recommendations.apply(
    lambda row: analizar_coincidencia_sinopsis(row["userId"], row["Synopsis"]),
    axis=1
)

# Resumen de calidad
print("\nResumen de calidad de recomendaciones:")
generos_relevantes = recommendations["GenerosCoinciden"].sum()
sinopsis_relevantes = recommendations["SinopsisCoinciden"].sum()
total_recomendaciones = len(recommendations)

print(f"- Recomendaciones con géneros relevantes: {generos_relevantes}/{total_recomendaciones} "
      f"({(generos_relevantes / total_recomendaciones) * 100:.2f}%)")
print(f"- Recomendaciones con sinopsis relevantes: {sinopsis_relevantes}/{total_recomendaciones} "
      f"({(sinopsis_relevantes / total_recomendaciones) * 100:.2f}%)")

# Guardar resultados
recommendations.to_csv(OUTPUT_ANALYSIS_FILE, sep=";", index=False)
print(f"Análisis guardado en: {OUTPUT_ANALYSIS_FILE}")
