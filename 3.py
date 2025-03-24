import boto3
import json
import logging
import math
import os
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración de DigitalOcean Spaces
DO_ACCESS_KEY = "DO00WC26X8H2CFGZCWC8"
DO_SECRET_KEY = "fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc"
DO_ENDPOINT = "https://nyc3.digitaloceanspaces.com"
DO_BUCKET_NAME = "bb-bubbo"

# Configuración de BigQuery
PROJECT_ID = "bubbo-dfba0"
DATASET_ID = "bbmedia"
CREDENTIALS_PATH = "credentials/bubbo-dfba0-47e395cdcdc7.json"

# Configuración del Logging
LOG_LEVEL = logging.INFO  # Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(processName)s - %(threadName)s - %(message)s"  # Formato del log

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)  # Logger específico para este módulo

# Archivo para el punto de control
CHECKPOINT_FILE = "checkpoint.json"
# Archivo de registro de inserciones
INSERTIONS_LOG_FILE = "insertions.log"

class DOToBigQueryTransfer:
    def __init__(self):
        self.session = boto3.session.Session()
        self.s3_client = self.session.client(
            's3',
            endpoint_url=DO_ENDPOINT,
            aws_access_key_id=DO_ACCESS_KEY,
            aws_secret_access_key=DO_SECRET_KEY
        )
        self.bucket_name = "bb-bubbo"
        self.bq_client = self.get_bigquery_client()
        self.checkpoint = self.load_checkpoint()
        self.insertions_logger = self.setup_insertions_logger()  # Nuevo logger para inserciones
        logger.info("DOToBigQueryTransfer initialized.")

    def setup_insertions_logger(self):
        """Configura un logger separado para registrar las inserciones."""
        insertions_logger = logging.getLogger("InsertionsLogger")
        insertions_logger.setLevel(logging.INFO)  # Nivel de logging específico
        fh = logging.FileHandler(INSERTIONS_LOG_FILE)  # Handler para escribir en el archivo
        formatter = logging.Formatter('%(asctime)s - %(message)s')  # Formato más simple
        fh.setFormatter(formatter)
        insertions_logger.addHandler(fh)
        return insertions_logger

    def get_bigquery_client(self):
        """Establece conexión con BigQuery."""
        logger.debug("Connecting to BigQuery...")
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        logger.info("Successfully connected to BigQuery.")
        return client

    def list_subdirectories(self):
        """Lista los subdirectorios en el bucket usando paginación"""
        subdirs = []
        logger.debug(f"Listing subdirectories in bucket: {self.bucket_name}")
        continuation_token = None

        while True:
            if continuation_token:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Delimiter='/', ContinuationToken=continuation_token)
            else:
                response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Delimiter='/')

            for prefix in response.get("CommonPrefixes", []):
                subdirs.append(prefix["Prefix"])

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        logger.info(f"Found subdirectories: {subdirs}")
        return sorted(subdirs)  # Devuelve los subdirectorios ordenados alfabéticamente

    def get_jsonl_structure(self, file_key):
        """Obtiene la estructura de un archivo JSONL sin cargar todo el contenido"""
        logger.debug(f"Getting JSONL structure for file: {file_key}")
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        first_line = response["Body"].readline().decode("utf-8")

        try:
            json_obj = json.loads(first_line)
            structure = {key: type(value).__name__ for key, value in json_obj.items()}
            logger.debug(f"JSONL structure for {file_key}: {structure}")
            return structure
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSONL for file: {file_key}")
            return None

    def extract_jsonl_structures(self):
        """Extrae la estructura JSONL de un archivo en cada subdirectorio"""
        subdirs = self.list_subdirectories()
        structures = {}

        for subdir in subdirs:
            logger.debug(f"Extracting JSONL structure for subdirectory: {subdir}")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=subdir, MaxKeys=10)
            for obj in response.get("Contents", []):
                file_key = obj["Key"]
                if file_key.endswith(".jsonl"):
                    structures[subdir] = self.get_jsonl_structure(file_key)
                    break  # Solo procesamos un archivo por subdirectorio
        logger.info(f"Extracted JSONL structures: {structures}")
        return structures

    def create_bigquery_schema(self, jsonl_structure):
        """Crea un esquema de BigQuery basado en la estructura JSONL"""
        logger.debug(f"Creating BigQuery schema from: {jsonl_structure}")
        schema = []
        if jsonl_structure:
            for key, value_type in jsonl_structure.items():
                if value_type == "str":
                    field_type = "STRING"
                elif value_type == "int":
                    field_type = "INTEGER"
                elif value_type == "float":
                    field_type = "FLOAT"
                elif value_type == "bool":
                    field_type = "BOOLEAN"
                elif value_type == "list":
                    field_type = "STRING"  # handle lists as strings
                elif value_type == "dict":
                    field_type = "STRING"  # handle dict as strings
                else:
                    field_type = "STRING"  # Default to STRING
                schema.append(bigquery.SchemaField(key, field_type))
            logger.debug(f"Created BigQuery schema: {schema}")
        else:
            logger.warning("JSONL structure is empty. Cannot create schema.")
        return schema

    def create_bigquery_tables(self):
        """Crea tablas de BigQuery basadas en la estructura JSONL"""
        structures = self.extract_jsonl_structures()
        for subdir, structure in structures.items():
            table_name = subdir.replace("/", "").replace("_", "")  # Clean up subdir name
            schema = self.create_bigquery_schema(structure)
            if schema:
                table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
                table = bigquery.Table(table_id, schema=schema)
                try:
                    self.bq_client.get_table(table_id)
                    logger.info(f"Table {table_id} already exists.")
                except Exception:
                    self.bq_client.create_table(table)
                    logger.info(f"Table {table_id} created.")
            else:
                logger.warning(f"Skipping {subdir} due to empty schema.")

    def load_checkpoint(self):
        """Carga el punto de control desde un archivo JSON."""
        logger.debug(f"Loading checkpoint from {CHECKPOINT_FILE}")
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                checkpoint = json.load(f)
                logger.info(f"Loaded checkpoint: {checkpoint}")
                return checkpoint
        logger.info("Checkpoint file not found. Starting fresh.")
        return {}

    def save_checkpoint(self):
        """Guarda el punto de control actual en un archivo JSON."""
        logger.debug(f"Saving checkpoint to {CHECKPOINT_FILE}")
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(self.checkpoint, f)
        logger.info(f"Checkpoint saved: {self.checkpoint}")

    def insert_jsonl_data(self, file_key, table_id):
        """Inserta datos de un archivo JSONL línea por línea en BigQuery."""
        logger.info(f"Starting data insertion for {file_key} into {table_id}...")
        try:
            local_file_path = f"/tmp/{file_key.replace('/', '_')}"
            logger.debug(f"Downloading file {file_key} to {local_file_path}")
            self.s3_client.download_file(self.bucket_name, file_key, local_file_path)
            table = self.bq_client.get_table(table_id)
            logger.debug(f"Table {table_id} retrieved.")

            with open(local_file_path, "r") as f:
                last_processed_line = self.checkpoint.get(file_key, 0)
                logger.info(f"Last processed line for {file_key}: {last_processed_line}")

                for i, line in enumerate(f):
                    if i < last_processed_line:
                        logger.debug(f"Skipping line {i} (already processed).")
                        continue

                    try:
                        data = json.loads(line)
                        for key, value in data.items():
                            if isinstance(value, float) and math.isnan(value):
                                data[key] = None
                            if isinstance(value, list) or isinstance(value, dict):
                                data[key] = str(value)
                            if isinstance(value, str) and "T00:00:00" in value:
                                pass
                            if key == "releasetype" and isinstance(value, str):
                                try:
                                    data[key] = float(value)
                                except ValueError:
                                    data[key] = None

                        logger.debug(f"Inserting data: {data}")
                        errors = self.bq_client.insert_rows_json(table, [data])
                        if errors:
                            logger.error(f"Error inserting row: {errors} for data: {data}")
                        else:
                            logger.info(f"Successfully inserted row {i} from {file_key}.")
                            # Registrar la inserción exitosa en el archivo de inserciones
                            self.insertions_logger.info(f"Inserted into {table_id}: {data}")
                            self.checkpoint[file_key] = i + 1
                            self.checkpoint['last_copied_file'] = file_key  # Actualizar el último archivo copiado
                            self.save_checkpoint()

                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON at line {i+1} in {file_key}: {line.strip()}")
                    except Exception as e:
                        logger.exception(f"Error processing line {i+1} in {file_key}: {line.strip()}.  Error: {e}")
            logger.debug(f"Removing local file: {local_file_path}")
            os.remove(local_file_path)
            logger.info(f"Finished processing {file_key}.")

        except Exception as e:
            logger.exception(f"Error processing {file_key}: {e}")

    def process_spaces_data(self, selected_subdir=None):
        """Procesa archivos JSONL de DigitalOcean Spaces a BigQuery."""
        logger.info("Starting data processing from DigitalOcean Spaces...")
        subdirs = self.list_subdirectories()

        if selected_subdir is not None:
            if 0 <= selected_subdir < len(subdirs):
                subdirs = [subdirs[selected_subdir]]
            else:
                logger.error("Selected subdirectory index is out of range.")
                return

        for subdir in subdirs:
            logger.info(f"Processing subdirectory: {subdir}")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=subdir)
            for obj in response.get("Contents", []):
                file_key = obj["Key"]
                if file_key.endswith(".jsonl"):
                    table_name = subdir.replace("/", "").replace("_", "")
                    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
                    logger.info(f"Processing file {file_key} and inserting into {table_id}")
                    self.insert_jsonl_data(file_key, table_id)

        logger.info("Finished processing data from DigitalOcean Spaces.")

# Uso
if __name__ == "__main__":
    do_transfer = DOToBigQueryTransfer()
    do_transfer.create_bigquery_tables()

    subdirs = do_transfer.list_subdirectories()
    print("Available subdirectories:")
    for idx, subdir in enumerate(subdirs):
        print(f"{idx}: {subdir}")

    selected_index = int(input("Enter the number of the subdirectory to process: "))
    do_transfer.process_spaces_data(selected_index)
