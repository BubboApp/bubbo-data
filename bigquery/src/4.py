import boto3
import json
import logging
import math
import os
import time
import gc
from google.cloud import bigquery
from google.oauth2 import service_account

# DigitalOcean Spaces Configuration
DO_ACCESS_KEY = "DO00WC26X8H2CFGZCWC8"
DO_SECRET_KEY = "fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc"
DO_ENDPOINT = "https://nyc3.digitaloceanspaces.com"
DO_BUCKET_NAME = "bb-bubbo"

# BigQuery Configuration
PROJECT_ID = "bubbo-dfba0"
DATASET_ID = "bbmedia"
CREDENTIALS_PATH = "credentials/bubbo-dfba0-47e395cdcdc7.json"

# Logging Configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(processName)s - %(threadName)s - %(message)s"

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Checkpoint and Logging Files
CHECKPOINT_FILE = "checkpoint.json"
INSERTIONS_LOG_FILE = "insertions.log"

class DOToBigQueryTransfer:
    def __init__(self, batch_size=500):
        """
        Initialize the transfer process with DigitalOcean Spaces and BigQuery clients
        """
        # DigitalOcean Spaces Client Setup
        self.session = boto3.session.Session()
        self.s3_client = self.session.client(
            's3',
            endpoint_url=DO_ENDPOINT,
            aws_access_key_id=DO_ACCESS_KEY,
            aws_secret_access_key=DO_SECRET_KEY
        )
        self.bucket_name = DO_BUCKET_NAME

        # BigQuery Client Setup
        self.bq_client = self.get_bigquery_client()

        # Checkpoint and Logging
        self.checkpoint = self.load_checkpoint()
        self.insertions_logger = self.setup_insertions_logger()
        
        # Batch Processing Configuration
        self.batch_size = batch_size
        
        logger.info("DOToBigQueryTransfer initialized successfully.")

    def load_checkpoint(self):
        """
        Load checkpoint data from a JSON file.
        If the file doesn't exist, return an empty dictionary.
        """
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, 'r') as f:
                    checkpoint = json.load(f)
                logger.info(f"Checkpoint loaded from {CHECKPOINT_FILE}")
                return checkpoint
            else:
                logger.info("No checkpoint file found. Starting from scratch.")
                return {}
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return {}

    def save_checkpoint(self):
        """
        Save checkpoint data to a JSON file.
        """
        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(self.checkpoint, f, indent=4)
            logger.info(f"Checkpoint saved to {CHECKPOINT_FILE}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def list_subdirectories(self):
        """
        List all subdirectories in the DigitalOcean Spaces bucket
        """
        subdirs = set()
        logger.debug(f"Listing subdirectories in bucket: {self.bucket_name}")
        
        continuation_token = None
        while True:
            try:
                # Use continuation token for pagination
                if continuation_token:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name, 
                        ContinuationToken=continuation_token
                    )
                else:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name
                    )

                # Extract unique directories from object keys
                for obj in response.get("Contents", []):
                    file_key = obj["Key"]
                    # Split the key and get all parent directories
                    parts = file_key.split('/')
                    
                    # Add all parent directories except the last (which is the file)
                    if len(parts) > 1:
                        # Add directories with trailing slash
                        for i in range(1, len(parts)):
                            subdir = '/'.join(parts[:i]) + '/'
                            subdirs.add(subdir)

                # Check for more results
                continuation_token = response.get("NextContinuationToken")
                if not continuation_token:
                    break

            except Exception as e:
                logger.error(f"Error listing subdirectories: {e}")
                break

        # Convert to sorted list
        sorted_subdirs = sorted(list(subdirs))
        
        logger.info(f"Found {len(sorted_subdirs)} unique subdirectories")
        for subdir in sorted_subdirs:
            logger.debug(f"Subdirectory: {subdir}")
        
        return sorted_subdirs

    def menu_seleccion_subdirectorios(self, subdirectorios, elementos_por_pagina=10):
        """
        Interactive menu for selecting subdirectories with pagination
        """
        while True:
            # Calculate total pages
            total_paginas = math.ceil(len(subdirectorios) / elementos_por_pagina)
            pagina_actual = 1

            while True:
                # Calculate start and end indices for current page
                indice_inicio = (pagina_actual - 1) * elementos_por_pagina
                indice_fin = indice_inicio + elementos_por_pagina
                subdirectorios_pagina = subdirectorios[indice_inicio:indice_fin]

                # Display menu
                print("\n--- DigitalOcean Subdirectories ---")
                print(f"Page {pagina_actual} of {total_paginas}")
                print("0. Process ALL subdirectories")
                for i, subdirectorio in enumerate(subdirectorios_pagina, 1):
                    print(f"{i}. {subdirectorio}")
                
                # Navigation options
                if pagina_actual > 1:
                    print("A. Previous page")
                if pagina_actual < total_paginas:
                    print("S. Next page")
                print("Q. Exit")

                # Get user input
                eleccion = input("\nEnter your choice: ").strip().upper()

                if eleccion == 'Q':
                    return []
                elif eleccion == 'A' and pagina_actual > 1:
                    pagina_actual -= 1
                elif eleccion == 'S' and pagina_actual < total_paginas:
                    pagina_actual += 1
                elif eleccion == '0':
                    return subdirectorios
                else:
                    try:
                        indice = int(eleccion)
                        if 1 <= indice <= len(subdirectorios_pagina):
                            subdirectorio_seleccionado = subdirectorios_pagina[indice - 1]
                            return [subdirectorio_seleccionado]
                    except ValueError:
                        print("Invalid input. Please try again.")

    def get_jsonl_structure(self, file_key):
        """
        Get the structure of the first line in a JSONL file
        """
        logger.debug(f"Getting JSONL structure for file: {file_key}")
        try:
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
            finally:
                # Close response body
                response['Body'].close()
        except Exception as e:
            logger.error(f"Error retrieving JSONL structure for {file_key}: {e}")
            return None

    def create_bigquery_schema(self, jsonl_structure):
        """
        Create BigQuery schema from JSONL structure
        """
        logger.debug(f"Creating BigQuery schema from: {jsonl_structure}")
        schema = []
        if jsonl_structure:
            for key, value_type in jsonl_structure.items():
                # Map Python types to BigQuery types
                if value_type == "str":
                    field_type = "STRING"
                elif value_type == "int":
                    field_type = "INTEGER"
                elif value_type == "float":
                    field_type = "FLOAT"
                elif value_type == "bool":
                    field_type = "BOOLEAN"
                elif value_type in ["list", "dict"]:
                    field_type = "STRING"
                else:
                    field_type = "STRING"
                
                schema.append(bigquery.SchemaField(key, field_type))
            
            logger.debug(f"Created BigQuery schema: {schema}")
            return schema
        else:
            logger.warning("JSONL structure is empty. Cannot create schema.")
            return []

    def create_bigquery_tables(self, subdirectorios_seleccionados):
        """
        Create BigQuery tables for selected subdirectories
        """
        for subdir in subdirectorios_seleccionados:
            # Get JSONL structure to determine table schema
            table_name = subdir.replace("/", "").replace("_", "")
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            
            try:
                # Get first JSONL file to determine schema
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, 
                    Prefix=subdir, 
                    MaxKeys=10
                )
                
                # Find first JSONL file
                jsonl_structure = None
                for obj in response.get("Contents", []):
                    if obj["Key"].endswith(".jsonl"):
                        jsonl_structure = self.get_jsonl_structure(obj["Key"])
                        break
                
                # Create table if structure found
                if jsonl_structure:
                    schema = self.create_bigquery_schema(jsonl_structure)
                    table = bigquery.Table(table_id, schema=schema)
                    
                    # Create table (replace if exists)
                    self.bq_client.create_table(table, exists_ok=True)
                    logger.info(f"Created or updated table: {table_id}")
                else:
                    logger.warning(f"Could not determine schema for {subdir}")
            
            except Exception as e:
                logger.error(f"Error creating table for {subdir}: {e}")

    def setup_insertions_logger(self):
        """
        Configure a separate logger for tracking insertions
        """
        insertions_logger = logging.getLogger("InsertionsLogger")
        insertions_logger.setLevel(logging.INFO)
        
        # Create file handler
        fh = logging.FileHandler(INSERTIONS_LOG_FILE)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        fh.setFormatter(formatter)
        insertions_logger.addHandler(fh)
        
        return insertions_logger

    def get_bigquery_client(self):
        """
        Create and return a BigQuery client using service account credentials
        """
        try:
            logger.debug("Connecting to BigQuery...")
            credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
            logger.info("Successfully connected to BigQuery.")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            raise

    def process_files(self, selected_subdirs=None):
        """
        Process files for selected subdirectories
        """
        if selected_subdirs is None:
            selected_subdirs = self.list_subdirectories()

        for subdir in selected_subdirs:
            table_name = subdir.replace("/", "").replace("_", "")
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            
            # List objects in the subdirectory
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=subdir)
            
            try:
                for obj in response.get("Contents", []):
                    file_key = obj["Key"]
                    if file_key.endswith(".jsonl") and file_key not in self.checkpoint:
                        self.insert_jsonl_data(file_key, table_id)
            
            finally:
                # Free resources
                del response
                gc.collect()

    def insert_jsonl_data(self, file_key, table_id):
        """
        Insert JSONL data into BigQuery
        """
        logger.info(f"Starting data insertion for {file_key} into {table_id}...")
        local_file_path = f"/tmp/{file_key.replace('/', '_')}"
        
        try:
            # Download file
            logger.debug(f"Downloading file {file_key} to {local_file_path}")
            self.s3_client.download_file(self.bucket_name, file_key, local_file_path)

            # Batch insertion to handle large files
            with open(local_file_path, "r") as f:
                batch = []
                for line in f:
                    try:
                        row = json.loads(line)
                        batch.append(row)
                        
                        # Insert batch when it reaches specified size
                        if len(batch) >= self.batch_size:
                            self._insert_batch(table_id, batch, file_key)
                            batch = []  # Reset batch
                            gc.collect()  # Force garbage collection
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON line: {e}")
                        continue

                # Insert final batch if anything remains
                if batch:
                    self._insert_batch(table_id, batch, file_key)

            # Remove temporary file
            os.remove(local_file_path)
            logger.info(f"Completed processing {file_key}")

        except Exception as e:
            logger.error(f"Failed to process {file_key}: {e}")
            self.insertions_logger.error(f"Failed to process {file_key}: {e}")
        finally:
            # Try to remove temporary file if it still exists
            try:
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file: {cleanup_error}")

    def _insert_batch(self, table_id, batch, file_key):
        """
        Insert a batch of data and handle errors
        """
        try:
            errors = self.bq_client.insert_rows_json(table_id, batch)
            if errors:
                logger.error(f"Errors occurred while inserting batch from {file_key}: {errors}")
                self.insertions_logger.error(f"Errors occurred while inserting batch from {file_key}: {errors}")
            else:
                logger.info(f"Successfully inserted batch from {file_key}")
                self.insertions_logger.info(f"Successfully inserted batch from {file_key}")
                
                # Update checkpoint with the last processed entry
                last_entry = batch[-1]
                self.checkpoint[file_key] = {
                    "status": "partially_inserted", 
                    "timestamp": math.floor(time.time()),
                    "last_entry": last_entry
                }
                self.save_checkpoint()

        except Exception as e:
            logger.error(f"Batch insertion error for {file_key}: {e}")
            self.insertions_logger.error(f"Batch insertion error for {file_key}: {e}")
        finally:
            # Clear memory
            del batch
            gc.collect()

    def procesar_subdirectorios_seleccionados(self):
        """
        Interactive process to select and transfer subdirectories
        """
        # Get list of subdirectories
        subdirectorios = self.list_subdirectories()
        
        if not subdirectorios:
            print("No subdirectories found.")
            return

        # Let user select subdirectories
        subdirectorios_seleccionados = self.menu_seleccion_subdirectorios(subdirectorios)
        
        if not subdirectorios_seleccionados:
            print("No subdirectories selected. Exiting.")
            return

        try:
            # Create BigQuery tables for selected subdirectories
            self.create_bigquery_tables(subdirectorios_seleccionados)

            # Process files for selected subdirectories
            self.process_files(subdirectorios_seleccionados)
        
        finally:
            # Clean up memory
            gc.collect()

# Main execution
if __name__ == "__main__":
    transfer = DOToBigQueryTransfer()
    transfer.procesar_subdirectorios_seleccionados()
