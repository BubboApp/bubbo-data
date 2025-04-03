import pandas as pd
import os
from google.cloud import translate_v3 as translate
from google.cloud import bigquery
import time # Importado por si necesitas añadir pausas (time.sleep)
import logging
import math
import numpy as np # Importado para manejar NaN de forma explícita

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Gestión de Credenciales ---
# Se recomienda usar Application Default Credentials (ADC).
# Comenta o elimina la siguiente línea si usas ADC.
# Si necesitas especificar un archivo, descomenta y ajusta la ruta.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bubbo-dfba0-47e395cdcdc7.json"
# Si ejecutas 'gcloud auth application-default login' en tu terminal,
# o si corres en un entorno GCP, las credenciales se suelen detectar automáticamente.

# --- Clientes de Google Cloud ---
try:
    translate_client = translate.TranslationServiceClient()
    bq_client = bigquery.Client()
except Exception as e:
    logging.error(f"Error al inicializar los clientes de Google Cloud: {e}")
    logging.error("Asegúrate de que las credenciales estén configuradas correctamente (ADC o GOOGLE_APPLICATION_CREDENTIALS).")
    raise

project_id = "bubbo-dfba0"  # ID del proyecto de Bubbo
location = "global"        # Ubicación para la API de Traducción

# --- Constantes para los límites de la API de Traducción v3 ---
# Límite oficial es 30720, usamos un poco menos para seguridad.
MAX_CODEPOINTS_PER_REQUEST = 30000
# Límite en el número de textos por request (suele ser 1024, pero 100-500 es prudente)
MAX_ITEMS_PER_REQUEST = 500 # Puedes ajustar este valor si es necesario


# --- Funciones Auxiliares ---

def batch_generator(items, batch_size):
    """Genera lotes de una secuencia (lista, índice, etc.)."""
    l = len(items)
    if l == 0:
        return # No generar nada si la secuencia está vacía
    num_batches = math.ceil(l / batch_size)
    logging.debug(f"Generando {num_batches} lotes de tamaño {batch_size} para {l} items.")
    for i in range(0, l, batch_size):
        yield items[i:min(i + batch_size, l)]

# --- Funciones Principales ---

def translate_text_batch_indexed(texts_with_indices, project_id, location, target_language_code):
    """
    Traduce textos manteniendo la asociación con sus índices originales.
    Crea lotes dinámicamente para respetar los límites de codepoints (MAX_CODEPOINTS_PER_REQUEST)
    y número de items (MAX_ITEMS_PER_REQUEST) por request API.
    Ya NO omite textos individuales largos preemptivamente.
    Espera una lista de tuplas: [(indice1, texto1), (indice2, texto2), ...].
    Devuelve un diccionario: {indice_original: texto_traducido}.
    """
    parent = f"projects/{project_id}/locations/{location}"
    translated_dict = {}

    # 1. Filtrar textos no válidos o vacíos, pero mantener sus índices originales.
    #    *** SE ELIMINÓ LA COMPROBACIÓN PREVENTIVA PARA TEXTOS INDIVIDUALES LARGOS ***
    valid_entries = []
    indices_with_invalid_text = []
    for idx, text in texts_with_indices:
        # Solo se consideran inválidos los no-strings o strings vacíos/espacios.
        if isinstance(text, str) and text.strip():
            valid_entries.append((idx, text))
        else:
            indices_with_invalid_text.append(idx) # Estos son realmente inválidos/vacíos

    if not valid_entries:
        logging.warning("No se encontraron textos válidos (no vacíos) para traducir en este lote de DataFrame.")
        return {}

    # Ajuste en el log: ya no menciona "demasiado largos" como causa de omisión aquí.
    logging.info(f"Preparado para traducir {len(valid_entries)} textos válidos (de {len(texts_with_indices)} originales en el lote de DataFrame).")
    if indices_with_invalid_text:
        logging.info(f"Se omitirán {len(indices_with_invalid_text)} textos inválidos o vacíos.")

    # 2. Crear lotes para la API dinámicamente basados en límites
    current_api_batch_indices = []
    current_api_batch_texts = []
    current_codepoint_count = 0
    api_batch_counter = 0

    for original_index, text_to_translate in valid_entries:
        text_codepoints = len(text_to_translate) # len() en Python cuenta codepoints

        # Comprobar si añadir este texto excedería algún límite del LOTE TOTAL
        exceeds_codepoints = (current_codepoint_count + text_codepoints) > MAX_CODEPOINTS_PER_REQUEST
        exceeds_items = (len(current_api_batch_indices) + 1) > MAX_ITEMS_PER_REQUEST

        # Si se excede un límite Y el lote actual no está vacío, enviar el lote actual ANTES de añadir el nuevo texto
        if (exceeds_codepoints or exceeds_items) and current_api_batch_indices:
            api_batch_counter += 1
            logging.info(f"Enviando lote API {api_batch_counter} ({len(current_api_batch_indices)} textos, {current_codepoint_count} codepoints).")
            try:
                response = translate_client.translate_text(
                    request={
                        "parent": parent,
                        "contents": current_api_batch_texts,
                        "mime_type": "text/plain",
                        "target_language_code": target_language_code,
                    }
                )
                for idx, translation in zip(current_api_batch_indices, response.translations):
                    translated_dict[idx] = translation.translated_text
                logging.info(f"Lote API {api_batch_counter} traducido con éxito.")

            except Exception as e:
                if "codepoints" in str(e) and "400" in str(e):
                     logging.error(f"Error de TAMAÑO al traducir el lote API {api_batch_counter}: {e}. El lote tenía {len(current_api_batch_indices)} items y {current_codepoint_count} codepoints.")
                else:
                     logging.error(f"Error al traducir el lote API {api_batch_counter}: {e}. Se omitirán las traducciones para este lote.")

            # Reiniciar el lote actual
            current_api_batch_indices = []
            current_api_batch_texts = []
            current_codepoint_count = 0
            # Opcional: Pausa breve
            # time.sleep(0.2)

        # Añadir el texto actual al nuevo lote (o al lote existente si no se excedieron límites)
        # Ya no hay comprobación individual de longitud aquí.
        current_api_batch_indices.append(original_index)
        current_api_batch_texts.append(text_to_translate)
        current_codepoint_count += text_codepoints

    # 3. Enviar el último lote si queda alguno pendiente
    if current_api_batch_indices:
        api_batch_counter += 1
        logging.info(f"Enviando último lote API {api_batch_counter} ({len(current_api_batch_indices)} textos, {current_codepoint_count} codepoints).")
        try:
            response = translate_client.translate_text(
                request={
                    "parent": parent,
                    "contents": current_api_batch_texts,
                    "mime_type": "text/plain",
                    "target_language_code": target_language_code,
                }
            )
            for idx, translation in zip(current_api_batch_indices, response.translations):
                translated_dict[idx] = translation.translated_text
            logging.info(f"Último lote API {api_batch_counter} traducido con éxito.")
        except Exception as e:
             if "codepoints" in str(e) and "400" in str(e):
                 logging.error(f"Error de TAMAÑO al traducir el último lote API {api_batch_counter}: {e}. El lote tenía {len(current_api_batch_indices)} items y {current_codepoint_count} codepoints.")
             else:
                 logging.error(f"Error al traducir el último lote API {api_batch_counter}: {e}. Se omitirán las traducciones para este lote.")

    logging.info(f"Traducción del lote de DataFrame completada. {len(translated_dict)} textos traducidos exitosamente.")
    return translated_dict


def get_data_from_bigquery(query):
    """Ejecuta una consulta en BigQuery y devuelve un DataFrame."""
    logging.info(f"Ejecutando consulta a BigQuery: {query}")
    try:
        # Usar BQ Storage API si está instalada (pip install google-cloud-bigquery-storage) para descargas más rápidas
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=True)
        logging.info(f"Consulta ejecutada exitosamente, se obtuvieron {len(df)} filas.")
        return df
    except Exception as e:
        logging.error(f"Error al obtener datos de BigQuery: {e}")
        raise

def save_to_bigquery(df, table_id):
    """Guarda un DataFrame en una tabla de BigQuery, usando autodetect para el esquema."""
    logging.info(f"Guardando {len(df)} filas en BigQuery, tabla: {table_id}")
    try:
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Habilita la detección automática del esquema
            write_disposition="WRITE_TRUNCATE",  # Reemplaza la tabla si ya existe
        )
        load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Espera a que el trabajo termine
        table = bq_client.get_table(table_id) # Obtener metadatos de la tabla para confirmar
        logging.info(f"Datos cargados exitosamente en {table_id}. La tabla ahora tiene {table.num_rows} filas.")
    except Exception as e:
        logging.error(f"Error al cargar datos en BigQuery ({table_id}): {e}")
        raise

def translate_df_in_batches(df, columnas_a_traducir, project_id, location, target_language="es", batch_size_df=500):
    """
    Traduce columnas específicas de un DataFrame en lotes de DataFrame, manejando índices.
    Llama a translate_text_batch_indexed que maneja los lotes de API internamente.
    Devuelve un *nuevo* DataFrame con las columnas traducidas.
    """
    logging.info(f"Iniciando la traducción del DataFrame (en lotes de {batch_size_df} filas) para las columnas: {', '.join(columnas_a_traducir)}")
    df_translated = df.copy() # Trabajar sobre una copia

    for columna in columnas_a_traducir:
        if columna not in df_translated.columns:
            logging.warning(f"Columna '{columna}' no encontrada en el DataFrame. Se omitirá.")
            continue

        logging.info(f"Traduciendo columna '{columna}'...")
        # Crear una Serie para almacenar las traducciones, alineada con el índice original
        translated_series = pd.Series(index=df_translated.index, dtype=object)

        # Iterar sobre el DataFrame en lotes de filas (usando índices)
        total_rows = len(df_translated)
        processed_rows = 0
        num_df_batches = math.ceil(total_rows / batch_size_df)

        for i, batch_indices in enumerate(batch_generator(df_translated.index, batch_size_df)):
            logging.info(f"Procesando lote de DataFrame {i+1}/{num_df_batches} para columna '{columna}' (filas {processed_rows+1} a {min(processed_rows+len(batch_indices), total_rows)}).")

            # Preparar los textos del lote actual con sus índices
            texts_with_indices_batch = []
            for idx in batch_indices:
                 value = df_translated.loc[idx, columna]
                 texts_with_indices_batch.append((idx, value if pd.notna(value) else None)) # Pasa NaN/None como None

            # Llamar a la función de traducción
            translated_results_dict = translate_text_batch_indexed(
                texts_with_indices_batch,
                project_id,
                location,
                target_language
            )

            # Actualizar la serie de traducciones usando el diccionario de resultados
            if translated_results_dict:
                 update_series = pd.Series(translated_results_dict, dtype=object)
                 translated_series.update(update_series)
                 logging.info(f"Actualizadas {len(update_series)} traducciones en la Serie para el lote {i+1}.")
            else:
                 logging.info(f"No se obtuvieron traducciones para el lote {i+1} (posiblemente textos inválidos o error API).")

            processed_rows += len(batch_indices)

        # Rellenar los valores que no se tradujeron (permanecen NaN/None en translated_series)
        # con los valores originales de esa columna.
        original_values = df_translated[columna]
        df_translated[columna] = translated_series.combine_first(original_values)

        logging.info(f"Columna '{columna}' procesada completamente.")

    return df_translated

# --- Flujo Principal ---

def main():
    logging.info("Inicio del proceso de traducción de contenido.")
    start_time = time.time()

    try:
        # 1. Obtén datos desde BigQuery
        # Asegúrate de que la tabla y columnas existen. ¡QUITAR LIMIT PARA PRODUCCIÓN!
        query = "SELECT * FROM `bubbo-dfba0.content.best_content`" # LIMIT 1000"
        df = get_data_from_bigquery(query)

        if df.empty:
            logging.warning("No se obtuvieron datos de BigQuery. Terminando proceso.")
            return

        # 2. Define columnas y traduce el DataFrame en batches
        columnas_a_traducir = ["genres", "title", "synopsis"] # Columnas a traducir
        columnas_existentes = [col for col in columnas_a_traducir if col in df.columns]

        if not columnas_existentes:
             logging.error(f"Ninguna de las columnas especificadas ({', '.join(columnas_a_traducir)}) existe en el DataFrame. Abortando traducción.")
             return
        if len(columnas_existentes) < len(columnas_a_traducir):
             columnas_faltantes = set(columnas_a_traducir) - set(columnas_existentes)
             logging.warning(f"Las siguientes columnas no se encontraron y no serán traducidas: {', '.join(columnas_faltantes)}")

        # Llamar a la función de traducción actualizada
        # Ajusta batch_size_df según la memoria disponible y la eficiencia deseada
        translated_df = translate_df_in_batches(
            df,
            columnas_existentes, # Pasar solo las columnas que sí existen
            project_id,
            location,
            target_language="es",
            batch_size_df=500  # Nº de filas del DF a procesar antes de preparar un lote para traducir
        )

        # 3. Guarda los resultados traducidos en una nueva tabla de BigQuery
        new_table_id = "bubbo-dfba0.content.best_content_translated_py" # Nombre de la tabla destino
        save_to_bigquery(translated_df, new_table_id)

    except Exception as e:
        logging.exception("Proceso fallido debido a una excepción no controlada.")
    else:
        logging.info("Proceso completado con éxito.")
    finally:
        end_time = time.time()
        logging.info(f"Tiempo total de ejecución: {end_time - start_time:.2f} segundos.")


if __name__ == "__main__":
    main()