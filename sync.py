import boto3
import json
import requests
import logging
import concurrent.futures
import os
import math
import re
import time
from datetime import datetime

# Configuración de logging
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Crear un nombre de archivo de log con timestamp
log_filename = os.path.join(LOG_DIR, f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configurar el logger para escribir tanto en consola como en archivo
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# --- Configuración DigitalOcean Spaces ---
DO_ACCESS_KEY = 'DO00WC26X8H2CFGZCWC8'
DO_SECRET_KEY = 'fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc'
DO_ENDPOINT = 'https://nyc3.digitaloceanspaces.com'
DO_BUCKET_NAME = 'bb-bubbo'

# --- Configuración Firebase ---
FIREBASE_CONFIG_PATH = "/workspaces/python-12/firebase.json"
try:
    with open(FIREBASE_CONFIG_PATH, 'r') as f:
        firebase_config = json.load(f)
    logging.info("Configuración de Firebase cargada desde %s", FIREBASE_CONFIG_PATH)
except Exception as e:
    logging.error("Error al cargar configuración de Firebase: %s", e)
    firebase_config = {}

FIREBASE_DATABASE_URL = 'https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app'
FIREBASE_PATH = 'copia'  # Nueva colección "copia"

# Tamaño del lote para envío en Firebase y archivo de estado para actualización incremental.
BATCH_SIZE = 100
STATE_FILE = "state.json"
ERROR_CHECKPOINT_FILE = "error_checkpoint.json"  # Archivo para guardar el punto de recuperación

# Patrón para validar claves de Firebase
FIREBASE_KEY_PATTERN = re.compile(r'^[^.$#\[\]/\u0000-\u001F\u007F-\u009F]+$')

# Estadísticas globales
stats = {
    "total_files_processed": 0,
    "total_files_skipped": 0,
    "total_files_deleted": 0,
    "total_documents_processed": 0,
    "total_documents_updated": 0,
    "total_documents_deleted": 0,
    "total_errors": 0,
    "start_time": time.time(),
    "files_with_errors": set(),
    "recovered_errors": 0
}

# Número máximo de reintentos para operaciones fallidas
MAX_RETRIES = 3
# Tiempo de espera entre reintentos (en segundos)
RETRY_DELAY = 5


def is_valid_firebase_key(key):
    """
    Verifica si una clave es válida para Firebase.
    """
    return bool(FIREBASE_KEY_PATTERN.match(key))


def sanitize_firebase_key(key):
    """
    Sanitiza una clave para que sea válida en Firebase.
    """
    # Reemplazar caracteres no válidos por guiones bajos
    sanitized = re.sub(r'[.$#\[\]/]', '_', key)
    # Si la clave comienza con un punto o un signo de dólar, agregar un prefijo
    if sanitized.startswith('.') or sanitized.startswith('$'):
        sanitized = 'key_' + sanitized
    return sanitized


def clean_json(data):
    """
    Limpia un objeto JSON para garantizar que sea válido para Firebase:
      - Reemplaza valores no válidos (NaN, Infinity, -Infinity) por None.
      - Sanitiza claves no válidas para Firebase.
    """
    if isinstance(data, dict):
        cleaned_data = {}
        for key, value in data.items():
            # Sanitizar clave
            cleaned_key = sanitize_firebase_key(key)
            # Limpiar valor
            cleaned_data[cleaned_key] = clean_json(value)
        return cleaned_data
    elif isinstance(data, list):
        return [clean_json(item) for item in data]
    elif isinstance(data, float):
        # Reemplazar valores no válidos
        if math.isnan(data) or math.isinf(data):
            return None
        return data
    elif data is None or isinstance(data, (bool, int, str)):
        return data
    else:
        # Convertir otros tipos a string
        return str(data)


def update_firebase(file_key, batch, retries=0):
    """
    Actualiza Firebase mediante una solicitud PATCH, insertando o actualizando los documentos.
    Implementa reintentos en caso de error.
    Ahora usa la estructura de archivos como en el bucket.
    """
    # Sanitizar la clave del archivo para Firebase
    safe_file_key = sanitize_firebase_key(file_key)
    url = f"{FIREBASE_DATABASE_URL}/{FIREBASE_PATH}/{safe_file_key}.json"

    # Verificar que el batch no esté vacío
    if not batch:
        logging.warning("Intento de actualizar Firebase con un lote vacío.")
        return True  # Consideramos esto como éxito

    try:
        # Intentar serializar el batch para verificar que sea válido
        json_data = json.dumps(batch)

        # Enviar la solicitud a Firebase
        response = requests.patch(url, data=json_data, headers={"Content-Type": "application/json"}, timeout=30)

        if response.status_code != 200:
            logging.error("Error al actualizar Firebase. Código: %s. Respuesta: %s", response.status_code, response.text)
            # Registrar los primeros 500 caracteres del payload para depuración
            logging.error("Payload (primeros 500 caracteres): %s", json_data[:500])

            # Implementar reintento si no hemos excedido el máximo
            if retries < MAX_RETRIES:
                logging.info(f"Reintentando actualización (intento {retries+1}/{MAX_RETRIES}) en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
                return update_firebase(file_key, batch, retries + 1)

            stats["total_errors"] += 1
            return False
        else:
            logging.info("Lote de %d documentos insertado correctamente en Firebase para archivo %s.", len(batch), file_key)
            # Actualizar estadísticas
            if all(value is None for value in batch.values()):
                stats["total_documents_deleted"] += len(batch)
            else:
                stats["total_documents_updated"] += len(batch)
            return True
    except json.JSONDecodeError as e:
        logging.error("Error al serializar el batch a JSON: %s", e)
        # Registrar las claves del batch para depuración
        logging.error("Claves del batch: %s", list(batch.keys()))
        stats["total_errors"] += 1
        return False
    except Exception as e:
        logging.error("Excepción al actualizar Firebase: %s", e)

        # Implementar reintento si no hemos excedido el máximo
        if retries < MAX_RETRIES:
            logging.info(f"Reintentando actualización (intento {retries+1}/{MAX_RETRIES}) en {RETRY_DELAY} segundos...")
            time.sleep(RETRY_DELAY)
            return update_firebase(file_key, batch, retries + 1)

        stats["total_errors"] += 1
        return False


def save_error_checkpoint(obj_key, line_number, batch):
    """
    Guarda un punto de control en caso de error para poder continuar desde ahí.
    """
    checkpoint = {
        "obj_key": obj_key,
        "line_number": line_number,
        "batch": batch,
        "timestamp": datetime.now().isoformat()
    }

    try:
        with open(ERROR_CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f)
        logging.info(f"Punto de control guardado para {obj_key} en línea {line_number}")
    except Exception as e:
        logging.error(f"Error al guardar punto de control: {e}")


def load_error_checkpoint():
    """
    Carga el último punto de control guardado.
    """
    if not os.path.exists(ERROR_CHECKPOINT_FILE):
        return None

    try:
        with open(ERROR_CHECKPOINT_FILE, "r") as f:
            checkpoint = json.load(f)
        logging.info(f"Punto de control cargado: {checkpoint['obj_key']} en línea {checkpoint['line_number']}")
        return checkpoint
    except Exception as e:
        logging.error(f"Error al cargar punto de control: {e}")
        return None


def process_file(obj_key, last_modified, start_line=0, initial_batch=None):
    """
    Procesa un objeto (archivo) proveniente del bucket:
      - Se conecta y descarga el objeto.
      - Lee su contenido línea a línea (formato JSONL).
      - Envía documentos en lotes de tamaño BATCH_SIZE a Firebase.
      - Registra y retorna los ID de documentos generados para actualizar el estado.
      - Soporta recuperación desde un punto específico.
      - Ahora mantiene la estructura de archivos como en el bucket.
    """
    logging.info("[%s] Iniciando procesamiento desde línea %d...", obj_key, start_line)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        endpoint_url=DO_ENDPOINT,
    )
    try:
        obj = s3.get_object(Bucket=DO_BUCKET_NAME, Key=obj_key)
        logging.info("[%s] Objeto recuperado correctamente.", obj_key)
    except Exception as e:
        logging.error("[%s] Error al obtener el objeto: %s", obj_key, e)
        stats["total_errors"] += 1
        stats["files_with_errors"].add(obj_key)
        return None

    batch = initial_batch or {}
    processed_doc_ids = []
    counter = 0
    error_count = 0
    file_stats = {
        "lines_processed": 0,
        "lines_with_errors": 0,
        "documents_updated": 0
    }

    # Extraer el nombre del archivo sin ruta
    file_name = os.path.basename(obj_key)

    try:
        for line in obj["Body"].iter_lines():
            counter += 1

            # Saltar líneas hasta el punto de inicio
            if counter <= start_line:
                continue

            if line:
                try:
                    # Parsear la línea como JSON
                    line_str = line.decode('utf-8')
                    doc = json.loads(line_str)

                    # Limpiar el JSON
                    doc = clean_json(doc)

                    # Se define el identificador del documento. Si existe "id", se utiliza; si no, se genera.
                    doc_id = str(doc.get("id", f"line_{counter}"))

                    # Sanitizar el ID del documento
                    doc_id = sanitize_firebase_key(doc_id)

                    batch[doc_id] = doc
                    processed_doc_ids.append(doc_id)
                    file_stats["lines_processed"] += 1

                    if len(batch) >= BATCH_SIZE:
                        success = update_firebase(file_name, batch)
                        if not success:
                            # Guardar punto de control en caso de error
                            save_error_checkpoint(obj_key, counter, batch)
                            return None

                        file_stats["documents_updated"] += len(batch)
                        batch.clear()
                except json.JSONDecodeError as e:
                    error_count += 1
                    file_stats["lines_with_errors"] += 1
                    if error_count <= 5:  # Limitar el número de errores registrados
                        logging.error("[%s] Error al parsear JSON en línea %d: %s", obj_key, counter, e)
                        logging.error("[%s] Contenido de la línea: %s", obj_key, line_str[:100])

                    # Guardar punto de control en caso de error grave
                    if error_count % 10 == 0:
                        save_error_checkpoint(obj_key, counter, batch)

                    continue
                except Exception as e:
                    error_count += 1
                    file_stats["lines_with_errors"] += 1
                    if error_count <= 5:
                        logging.error("[%s] Error al procesar línea %d: %s", obj_key, counter, e)

                    # Guardar punto de control en caso de error grave
                    if error_count % 10 == 0:
                        save_error_checkpoint(obj_key, counter, batch)

                    continue

        if batch:
            success = update_firebase(file_name, batch)
            if not success:
                # Guardar punto de control en caso de error
                save_error_checkpoint(obj_key, counter, batch)
                return None

            file_stats["documents_updated"] += len(batch)
    except Exception as e:
        logging.error("[%s] Error al procesar el archivo: %s", obj_key, e)
        stats["total_errors"] += 1
        stats["files_with_errors"].add(obj_key)
        save_error_checkpoint(obj_key, counter, batch)
        return None

    if error_count > 0:
        logging.warning("[%s] Total de errores durante el procesamiento: %d", obj_key, error_count)
        stats["files_with_errors"].add(obj_key)

    # Actualizar estadísticas globales
    stats["total_documents_processed"] += file_stats["lines_processed"]
    stats["total_errors"] += file_stats["lines_with_errors"]
    stats["total_files_processed"] += 1

    logging.info("[%s] Procesamiento finalizado. Resumen: %d líneas procesadas, %d errores, %d documentos actualizados",
                obj_key, file_stats["lines_processed"], file_stats["lines_with_errors"], file_stats["documents_updated"])

    # Eliminar el punto de control si existe, ya que hemos completado el procesamiento
    if os.path.exists(ERROR_CHECKPOINT_FILE):
        try:
            os.remove(ERROR_CHECKPOINT_FILE)
            logging.info("Punto de control eliminado tras procesamiento exitoso")
        except Exception as e:
            logging.warning(f"No se pudo eliminar el punto de control: {e}")

    return processed_doc_ids


def delete_firebase_file(file_name):
    """
    Elimina un archivo completo de Firebase.
    """
    safe_file_key = sanitize_firebase_key(file_name)
    url = f"{FIREBASE_DATABASE_URL}/{FIREBASE_PATH}/{safe_file_key}.json"

    try:
        response = requests.delete(url, timeout=30)
        if response.status_code != 200:
            logging.error("Error al eliminar archivo %s de Firebase. Código: %s", file_name, response.status_code)
            return False
        logging.info("Archivo %s eliminado correctamente de Firebase.", file_name)
        return True
    except Exception as e:
        logging.error("Error al eliminar archivo %s de Firebase: %s", file_name, e)
        return False


def load_state():
    """
    Carga el estado previo desde STATE_FILE. El estado es un diccionario que mapea cada objeto
    del bucket a su 'last_modified' y lista de 'doc_ids' procesados.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error("Error al leer el archivo de estado: %s", e)
            stats["total_errors"] += 1
            return {}
    return {}


def save_state(state):
    """
    Guarda el estado actual en STATE_FILE.
    """
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
        logging.info("Archivo de estado actualizado correctamente.")
    except Exception as e:
        logging.error("Error al guardar el archivo de estado: %s", e)
        stats["total_errors"] += 1


def log_summary():
    """
    Registra un resumen de la ejecución en el archivo de log.
    """
    elapsed_time = time.time() - stats["start_time"]

    summary = [
        "=" * 80,
        "RESUMEN DE EJECUCIÓN",
        "=" * 80,
        f"Tiempo total de ejecución: {elapsed_time:.2f} segundos",
        f"Archivos procesados: {stats['total_files_processed']}",
        f"Archivos omitidos (sin cambios): {stats['total_files_skipped']}",
        f"Archivos eliminados: {stats['total_files_deleted']}",
        f"Documentos procesados: {stats['total_documents_processed']}",
        f"Documentos actualizados en Firebase: {stats['total_documents_updated']}",
        f"Documentos eliminados de Firebase: {stats['total_documents_deleted']}",
        f"Total de errores: {stats['total_errors']}",
        f"Errores recuperados: {stats['recovered_errors']}",
        f"Archivos con errores: {len(stats['files_with_errors'])}"
    ]

    if stats["files_with_errors"]:
        summary.append("Lista de archivos con errores:")
        for file in stats["files_with_errors"]:
            summary.append(f"  - {file}")

    summary.append("=" * 80)

    for line in summary:
        logging.info(line)

    # Crear un archivo de resumen separado
    summary_filename = os.path.join(LOG_DIR, f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    try:
        with open(summary_filename, "w") as f:
            for line in summary:
                f.write(line + "\n")
        logging.info(f"Resumen guardado en {summary_filename}")
    except Exception as e:
        logging.error(f"Error al guardar el archivo de resumen: {e}")


def check_for_recovery():
    """
    Verifica si hay un punto de control para recuperar y lo procesa.
    """
    checkpoint = load_error_checkpoint()
    if checkpoint:
        logging.info(f"Recuperando desde punto de control: {checkpoint['obj_key']} línea {checkpoint['line_number']}")

        # Obtener el último last_modified del objeto
        s3 = boto3.client(
            "s3",
            aws_access_key_id=DO_ACCESS_KEY,
            aws_secret_access_key=DO_SECRET_KEY,
            endpoint_url=DO_ENDPOINT,
        )

        try:
            obj_info = s3.head_object(Bucket=DO_BUCKET_NAME, Key=checkpoint['obj_key'])
            last_mod = obj_info['LastModified'].isoformat()

            # Procesar el archivo desde el punto de control
            doc_ids = process_file(
                checkpoint['obj_key'],
                last_mod,
                start_line=checkpoint['line_number'],
                initial_batch=checkpoint.get('batch', {})
            )

            if doc_ids:
                # Actualizar el estado con los nuevos doc_ids
                state = load_state()
                state[checkpoint['obj_key']] = {"last_modified": last_mod, "doc_ids": doc_ids}
                save_state(state)
                stats["recovered_errors"] += 1
                logging.info(f"Recuperación exitosa para {checkpoint['obj_key']}")
                return True
        except Exception as e:
            logging.error(f"Error durante la recuperación: {e}")

    return False


def incremental_update():
    """
    Realiza una actualización incremental:
      1. Carga el estado previo.
      2. Lista los objetos actuales del bucket (con LastModified).
      3. Detecta archivos nuevos o modificados para procesarlos.
      4. Detecta archivos eliminados para remover sus documentos asociados en Firebase.
      5. Actualiza el estado local.
    """
    logging.info("Iniciando actualización incremental...")

    # Verificar si hay un punto de control para recuperar
    recovered = check_for_recovery()
    if recovered:
        logging.info("Recuperación completada, continuando con actualización incremental...")

    state = load_state()  # Estructura: { "obj_key": {"last_modified": <...>, "doc_ids": [ ... ]}, ... }
    s3 = boto3.client(
        "s3",
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        endpoint_url=DO_ENDPOINT,
    )

    current_objects = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET_NAME):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                # Convierte el LastModified a ISO 8601
                last_mod = obj["LastModified"].isoformat()
                current_objects[key] = last_mod

    # Determinar archivos nuevos o modificados
    objects_to_update = []
    for key, last_mod in current_objects.items():
        if key not in state:
            logging.info("[%s] Archivo nuevo detectado.", key)
            objects_to_update.append((key, last_mod))
        elif state[key]["last_modified"] != last_mod:
            logging.info("[%s] Archivo modificado detectado.", key)
            # Para archivos modificados, primero eliminamos la versión anterior
            file_name = os.path.basename(key)
            delete_firebase_file(file_name)
            objects_to_update.append((key, last_mod))
        else:
            logging.info("[%s] Sin cambios detectados; se omite.", key)
            stats["total_files_skipped"] += 1

    # Determinar archivos eliminados
    files_to_delete = []
    for key in state:
        if key not in current_objects:
            logging.info("[%s] Archivo eliminado del bucket.", key)
            files_to_delete.append(key)

    # Actualizar el estado
    new_state = state.copy()

    # Procesar archivos nuevos o modificados
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_key = {
            executor.submit(process_file, key, last_mod): key
            for key, last_mod in objects_to_update
        }
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                doc_ids = future.result()
                if doc_ids:
                    new_state[key] = {"last_modified": current_objects[key], "doc_ids": doc_ids}
            except Exception as exc:
                logging.error("[%s] Excepción durante el procesamiento: %s", key, exc)
                stats["total_errors"] += 1
                stats["files_with_errors"].add(key)
                # Guardar punto de control para este archivo
                save_error_checkpoint(key, 0, {})

    # Procesar archivos eliminados
    for key in files_to_delete:
        file_name = os.path.basename(key)
        logging.info("[%s] Eliminando archivo de Firebase.", file_name)
        if delete_firebase_file(file_name):
            stats["total_files_deleted"] += 1
            new_state.pop(key, None)

    # Guardar el estado actualizado
    save_state(new_state)
    logging.info("Actualización incremental completada.")


if __name__ == "__main__":
    try:
        incremental_update()
    except KeyboardInterrupt:
        logging.warning("Ejecución interrumpida manualmente.")
    finally:
        # Registrar el resumen de la ejecución
        log_summary()