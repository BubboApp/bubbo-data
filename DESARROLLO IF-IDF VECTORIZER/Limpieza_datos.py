import pandas as pd

# --- CONFIGURACIÓN ---
USER_PREFERENCES_FILE = "user_preferences_final_cleaned.csv"
FILTERED_DATA_FILE = "filtered_data_final_cleaned.csv"
OUTPUT_USER_PREFERENCES_FILE = "user_preferences_final.csv"
OUTPUT_FILTERED_DATA_FILE = "filtered_data_final.csv"

# --- FUNCIONES DE LIMPIEZA ---

def limpiar_user_preferences(file_path, output_path):
    print("Cargando y limpiando user_preferences...")
    user_preferences = pd.read_csv(file_path, sep=";")

    # Columnas clave para verificar contenido
    columnas_clave = ['PeliculasFavoritas', 'SeriesFavoritas', 'GenerosFavoritos', 
                      'DetallesPeliculas', 'DetallesSeries']

    # Eliminar filas con valores nulos en todas las columnas clave
    user_preferences = user_preferences.dropna(subset=columnas_clave, how='all')

    # Eliminar filas donde TODAS las columnas clave sean listas vacías '[]'
    for columna in columnas_clave:
        user_preferences = user_preferences[
            user_preferences[columna].apply(lambda x: x != '[]')
        ]

    # Guardar archivo limpio
    user_preferences.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"Archivo limpio guardado en: {output_path}")


def limpiar_filtered_data(file_path, output_path):
    print("Cargando y limpiando filtered_data...")
    filtered_data = pd.read_csv(file_path, sep=";")

    # Eliminar registros duplicados basados en ExternalIds
    filtered_data = filtered_data.drop_duplicates(subset="ExternalIds")

    # Rellenar valores nulos en 'Synopsis' con un texto genérico
    filtered_data["Synopsis"].fillna("Sinopsis no disponible", inplace=True)

    # Guardar archivo limpio
    filtered_data.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"Archivo limpio guardado en: {output_path}")


# --- EJECUCIÓN ---

if __name__ == "__main__":
    # Limpiar user_preferences
    limpiar_user_preferences(USER_PREFERENCES_FILE, OUTPUT_USER_PREFERENCES_FILE)

    # Limpiar filtered_data
    limpiar_filtered_data(FILTERED_DATA_FILE, OUTPUT_FILTERED_DATA_FILE)

    print("\n=== LIMPIEZA COMPLETADA ===")
