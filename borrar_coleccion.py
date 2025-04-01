import requests
import logging
import os
import json
from datetime import datetime

# Configuración de logging
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename = os.path.join(LOG_DIR, f"delete_copia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# --- Configuración Firebase ---
FIREBASE_DATABASE_URL = 'https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app'
FIREBASE_PATH = 'copia'  # Colección a eliminar

def delete_collection():
    """
    Elimina la colección "copia" de Firebase.
    """
    url = f"{FIREBASE_DATABASE_URL}/{FIREBASE_PATH}.json"

    try:
        response = requests.delete(url, timeout=30)
        if response.status_code == 200:
            logging.info("Colección '%s' eliminada correctamente de Firebase.", FIREBASE_PATH)
        else:
            logging.error("Error al eliminar la colección '%s'. Código: %s. Respuesta: %s", FIREBASE_PATH, response.status_code, response.text)
    except Exception as e:
        logging.error("Error al eliminar la colección '%s': %s", FIREBASE_PATH, e)

if __name__ == "__main__":
    delete_collection()