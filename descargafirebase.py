import requests
import json
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import psutil
import os
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from rich.logging import RichHandler
import logging
from queue import Queue
from threading import Lock
import signal

# Configuración de Rich y logging
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger("rich")

# Configuración
FIREBASE_BASE_URL = "https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app/documents/"
JSON_FILE = "firebase_all_documents.json"
PROGRESS_FILE = "download_progress.json"  # Archivo para guardar el progreso
CHUNK_SIZE = 50  # Tamaño reducido para mejor control de memoria
MAX_RETRIES = 3
RETRY_DELAY = 1
WRITE_BUFFER_SIZE = 10  # Número de documentos antes de escribir a disco

class DocumentWriter:
    def __init__(self, filename):
        self.filename = filename
        self.lock = Lock()
        self.first_write = True
        self.buffer = []
        
        # Inicializar archivo
        with open(self.filename, 'w', encoding='utf-8') as f:
            f.write('[\n')
    
    def write_documents(self, documents):
        with self.lock:
            with open(self.filename, 'a', encoding='utf-8') as f:
                for doc in documents:
                    if not self.first_write:
                        f.write(',\n')
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                    self.first_write = False
    
    def finalize(self):
        with open(self.filename, 'a', encoding='utf-8') as f:
            f.write('\n]')

def get_optimal_workers():
    """Determina el número óptimo de workers basado en CPU y memoria"""
    cpu_count = multiprocessing.cpu_count()
    memory_usage = psutil.virtual_memory().percent
    
    if memory_usage > 90:
        return max(1, cpu_count // 4)
    elif memory_usage > 70:
        return max(1, cpu_count // 2)
    else:
        return cpu_count

def download_document(doc_name):
    """Descarga un documento individual con reintentos"""
    doc_url = f"{FIREBASE_BASE_URL}{doc_name}.json"
    
    for attempt in range(MAX_RETRIES):
        try:
            start_time = time.time()  # Tiempo de inicio de descarga
            response = requests.get(doc_url, timeout=10)
            download_time = time.time() - start_time  # Tiempo de descarga
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict):
                    data["document_name"] = doc_name
                    logger.info(f"Descargado {doc_name} en {download_time:.2f} segundos")
                    return data, download_time
            time.sleep(RETRY_DELAY)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Error final descargando {doc_name}: {e}")
            time.sleep(RETRY_DELAY)
    return None, 0

def load_progress():
    """Carga el progreso desde el archivo"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_progress(document_names):
    """Guarda el progreso en el archivo"""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(document_names, f)

def process_documents(document_names, writer, progress):
    """Procesa documentos en chunks y los escribe incrementalmente"""
    buffer = []
    total_processed = 0
    total_download_time = 0
    total_write_time = 0
    
    # Cargar progreso
    downloaded_documents = load_progress()
    logger.info(f"Progreso cargado: {len(downloaded_documents)} documentos ya descargados.")
    
    with ThreadPoolExecutor(max_workers=get_optimal_workers()) as executor:
        futures = {}
        
        for doc_name in document_names:
            if doc_name in downloaded_documents:
                logger.info(f"Saltando {doc_name}, ya descargado.")
                total_processed += 1
                progress.update(task, advance=1)
                continue
            
            future = executor.submit(download_document, doc_name)
            futures[future] = doc_name
            
            # Procesar resultados completados
            completed = [f for f in futures.keys() if f.done()]
            for future in completed:
                doc_name = futures[future]
                try:
                    result, download_time = future.result()
                    if result:
                        buffer.append(result)
                        total_download_time += download_time
                        downloaded_documents.append(doc_name)  # Agregar a la lista de descargados
                        save_progress(downloaded_documents)  # Guardar progreso
                        if len(buffer) >= WRITE_BUFFER_SIZE:
                            start_write_time = time.time()  # Tiempo de inicio de escritura
                            writer.write_documents(buffer)
                            write_time = time.time() - start_write_time  # Tiempo de escritura
                            total_write_time += write_time
                            logger.info(f"Escritos {len(buffer)} documentos en {write_time:.2f} segundos")
                            buffer.clear()
                except Exception as e:
                    logger.error(f"Error procesando {doc_name}: {e}")
                
                del futures[future]
                total_processed += 1
                progress.update(task, advance=1)
                
                # Monitorear uso de memoria
                memory_usage = psutil.Process().memory_info().rss / 1024 / 1024
                if memory_usage > 1000:  # Si uso de memoria > 1GB
                    logger.warning(f"Alto uso de memoria: {memory_usage:.2f}MB")
                    time.sleep(1)  # Pequeña pausa para liberar memoria
        
        # Procesar documentos restantes en el buffer
        if buffer:
            start_write_time = time.time()  # Tiempo de inicio de escritura
            writer.write_documents(buffer)
            write_time = time.time() - start_write_time  # Tiempo de escritura
            total_write_time += write_time
            logger.info(f"Escritos {len(buffer)} documentos en {write_time:.2f} segundos")
    
    # Calcular tasas
    if total_processed > 0:
        download_rate = total_download_time / total_processed
        write_rate = total_write_time / total_processed
        logger.info(f"Tasa de descarga promedio: {download_rate:.2f} segundos/documento")
        logger.info(f"Tasa de escritura promedio: {write_rate:.2f} segundos/documento")

def main():
    try:
        # Cargar nombres de documentos
        with open("fixed_document_names.json", "r", encoding="utf-8") as f:
            document_names = json.load(f)

        logger.info(f"Iniciando descarga de [bold green]{len(document_names)}[/] documentos")
        
        writer = DocumentWriter(JSON_FILE)
        
        with Progress(
            SpinnerColumn(),
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            global task
            task = progress.add_task("[cyan]Descargando...", total=len(document_names))
            process_documents(document_names, writer, progress)
        
        writer.finalize()
        logger.info(f"[bold green]✓[/] Proceso completado. Archivo guardado en {JSON_FILE}")

    except Exception as e:
        logger.exception(f"Error en el proceso principal: {e}")
        if os.path.exists(JSON_FILE):
            os.remove(JSON_FILE)

def handle_interrupt(signum, frame):
    logger.warning("\n[bold red]Interrupción detectada. Limpiando y saliendo...[/]")
    if os.path.exists(JSON_FILE):
        os.remove(JSON_FILE)
    exit(1)

if __name__ == "__main__":
    # Registrar manejador de señales para interrupciones
    signal.signal(signal.SIGINT, handle_interrupt)
    main()

# Created/Modified files during execution:
print("\nArchivos creados/modificados:")
print(f"- {JSON_FILE}")
print(f"- {PROGRESS_FILE}")