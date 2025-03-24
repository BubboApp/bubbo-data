#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
import os
import json
import time
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from botocore.client import Config
import tempfile
import pickle
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transfer_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DOToBigQueryTransfer:
    def __init__(self, credentials_path, checkpoint_file="checkpoint.pkl", max_retries=3, retry_delay=5):
        # Configuración de Digital Ocean Spaces
        self.do_access_key = 'DO00WC26X8H2CFGZCWC8'
        self.do_secret_key = 'fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc'
        self.do_endpoint = 'https://nyc3.digitaloceanspaces.com'
        self.do_bucket_name = 'bb-bubbo'
        
        # Configuración de BigQuery
        self.project_id = 'bubbo-dfba0'
        self.dataset_id = 'bbmedia'
        self.location = 'europe-southwest1'
        self.credentials_path = credentials_path
        
        # Configuración de checkpoints y reintentos
        self.checkpoint_file = checkpoint_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Inicializar clientes
        self.s3_client = self._get_s3_client()
        self.bq_client = self._get_bigquery_client()
        
        # Estadísticas de procesamiento
        self.stats = {
            'processed_files': 0,
            'failed_files': 0,
            'processed_lines': 0,
            'invalid_lines': 0,
            'start_time': datetime.now(),
            'last_processed_key': None,
            'last_continuation_token': None,
            'processed_keys': set()
        }
        
        # Cargar checkpoint si existe
        self._load_checkpoint()
    
    def _get_s3_client(self):
        """Inicializa y retorna un cliente S3 para Digital Ocean Spaces"""
        session = boto3.session.Session()
        return session.client(
            's3',
            endpoint_url=self.do_endpoint,
            aws_access_key_id=self.do_access_key,
            aws_secret_access_key=self.do_secret_key,
            config=Config(signature_version='s3v4')
        )
    
    def _get_bigquery_client(self):
        """Inicializa y retorna un cliente de BigQuery usando archivo de credenciales"""
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        return bigquery.Client(
            credentials=credentials, 
            project=self.project_id,
            location=self.location
        )
    
    def _save_checkpoint(self):
        """Guarda el estado actual del procesamiento"""
        try:
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(self.stats, f)
            logger.info(f"🔖 Checkpoint guardado en {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"❌ Error al guardar checkpoint: {str(e)}")
    
    def _load_checkpoint(self):
        """Carga el último checkpoint guardado si existe"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    self.stats = pickle.load(f)
                logger.info(f"📂 Checkpoint cargado desde {self.checkpoint_file}")
                logger.info(f"🔄 Reanudando desde el archivo: {self.stats['last_processed_key']}")
                logger.info(f"📊 Archivos procesados: {self.stats['processed_files']}, Fallidos: {self.stats['failed_files']}")
                return True
            except Exception as e:
                logger.error(f"❌ Error al cargar checkpoint: {str(e)}")
                return False
        return False
    
    def _clear_checkpoint(self):
        """Elimina el archivo de checkpoint"""
        if os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                logger.info(f"🗑️ Checkpoint eliminado: {self.checkpoint_file}")
            except Exception as e:
                logger.error(f"❌ Error al eliminar checkpoint: {str(e)}")
    
    def list_objects(self, prefix="", continuation_token=None, max_keys=1000):
        """Lista objetos en el bucket con paginación"""
        params = {
            'Bucket': self.do_bucket_name,
            'Prefix': prefix,
            'MaxKeys': max_keys
        }
        
        if continuation_token:
            params['ContinuationToken'] = continuation_token
            
        return self.s3_client.list_objects_v2(**params)
    
    def download_object(self, key, local_path, retries=0):
        """Descarga un objeto del bucket a una ruta local con reintentos"""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            self.s3_client.download_file(self.do_bucket_name, key, local_path)
            return local_path
        except Exception as e:
            if retries < self.max_retries:
                logger.warning(f"⚠️ Error al descargar {key}: {str(e)}. Reintentando ({retries+1}/{self.max_retries})...")
                time.sleep(self.retry_delay)
                return self.download_object(key, local_path, retries + 1)
            else:
                logger.error(f"❌ Error al descargar {key} después de {self.max_retries} intentos: {str(e)}")
                raise
    
    def _is_jsonl_file(self, key):
        """Determina si el archivo es JSONL basado en su extensión"""
        lower_key = key.lower()
        return lower_key.endswith('.jsonl') or lower_key.endswith('.json')
    
    def _create_table_name_from_key(self, key):
        """Crea un nombre de tabla válido para BigQuery a partir de la clave del objeto"""
        # Extraer directorio y nombre de archivo
        dir_path, filename = os.path.split(key)
        # Eliminar extensión del archivo
        base_name = os.path.splitext(filename)[0]
        
        # Crear nombre de tabla incluyendo la estructura de directorios
        if dir_path:
            # Reemplazar '/' con '_' para representar la estructura de directorios
            dir_part = dir_path.replace('/', '_')
            table_name = f"{dir_part}_{base_name}"
        else:
            table_name = base_name
        
        # Reemplazar caracteres no válidos
        table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
        
        # Asegurarse de que no comience con un número
        if table_name and table_name[0].isdigit():
            table_name = 'tbl_' + table_name
            
        return table_name
    
    def _extract_valid_json_lines(self, input_file_path, output_file_path):
        """Extrae las líneas JSON válidas de un archivo JSONL, ignorando las inválidas"""
        valid_lines = 0
        invalid_lines = 0
        
        with open(input_file_path, 'r', encoding='utf-8', errors='replace') as infile, open(output_file_path, 'w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:  # Ignorar líneas vacías
                    continue
                
                try:
                    # Intentar analizar la línea como JSON
                    json_obj = json.loads(line)
                    # Si el análisis tiene éxito, escribir el objeto JSON formateado correctamente
                    outfile.write(json.dumps(json_obj) + '\n')
                    valid_lines += 1
                except json.JSONDecodeError as e:
                    invalid_lines += 1
                    if invalid_lines <= 5:  # Limitar la cantidad de errores mostrados para no saturar la consola
                        logger.warning(f"⚠️ Error en línea {line_num}: {e} - '{line[:100]}...'")
                    elif invalid_lines == 6:
                        logger.warning("⚠️ Más errores encontrados. No se mostrarán todos por brevedad.")
        
        # Actualizar estadísticas
        self.stats['processed_lines'] += valid_lines
        self.stats['invalid_lines'] += invalid_lines
        
        logger.info(f"📄 Archivo {os.path.basename(input_file_path)} procesado: {valid_lines} líneas válidas, {invalid_lines} líneas inválidas omitidas")
        
        return valid_lines  # Retornar el número de líneas válidas
    
    def _load_jsonl_to_bigquery(self, local_file_path, key, retries=0):
        """Carga el archivo JSONL a BigQuery, procesando solo las líneas válidas"""
        # Crear un archivo temporal para las líneas válidas
        with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as temp_file:
            validated_file_path = temp_file.name
        
        try:
            # Extraer solo las líneas JSON válidas
            valid_lines = self._extract_valid_json_lines(local_file_path, validated_file_path)
            
            if valid_lines == 0:
                logger.warning(f"⚠️ No se encontraron líneas JSON válidas en {key}")
                return False
            
            table_name = self._create_table_name_from_key(key)
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Configurar el trabajo de carga para JSONL
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,  # Detecta automáticamente el esquema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Sobrescribe si la tabla existe
                ignore_unknown_values=True  # Ignora valores desconocidos
            )
            
            # Cargar los datos desde el archivo con líneas válidas
            with open(validated_file_path, "rb") as source_file:
                job = self.bq_client.load_table_from_file(
                    source_file, table_id, job_config=job_config
                )
            
            # Esperar a que el trabajo termine
            job.result()
            
            logger.info(f"✅ Cargado {key} a {table_id} ({valid_lines} registros)")
            return True
            
        except Exception as e:
            if retries < self.max_retries:
                logger.warning(f"⚠️ Error al cargar {key} a BigQuery: {str(e)}. Reintentando ({retries+1}/{self.max_retries})...")
                time.sleep(self.retry_delay)
                return self._load_jsonl_to_bigquery(local_file_path, key, retries + 1)
            else:
                logger.error(f"❌ Error al cargar {key} a BigQuery después de {self.max_retries} intentos: {str(e)}")
                return False
            
        finally:
            # Eliminar el archivo temporal
            if os.path.exists(validated_file_path):
                os.remove(validated_file_path)
    
    def process_bucket(self, prefix="", temp_dir="./temp_files", save_checkpoint_interval=10):
        """Procesa todos los objetos JSONL en el bucket utilizando paginación"""
        # Crear directorio temporal si no existe
        os.makedirs(temp_dir, exist_ok=True)
        
        continuation_token = self.stats.get('last_continuation_token')
        processed_files_checkpoint = 0
        
        logger.info(f"🔍 Iniciando procesamiento de archivos en bucket {self.do_bucket_name} con prefijo '{prefix}'")
        
        while True:
            try:
                # Obtener una página de objetos
                response = self.list_objects(prefix=prefix, continuation_token=continuation_token)
                
                # Procesar objetos en esta página
                for obj in response.get('Contents', []):
                    key = obj['Key']
                    
                    # Actualizar último archivo procesado para el checkpoint
                    self.stats['last_processed_key'] = key
                    
                    # Saltamos directorios (objetos que terminan con /)
                    if key.endswith('/'):
                        continue
                    
                    # Solo procesamos archivos JSONL
                    if not self._is_jsonl_file(key):
                        continue
                    
                    # Si ya procesamos este archivo (según el checkpoint), lo omitimos
                    if key in self.stats['processed_keys']:
                        logger.info(f"⏭️ Omitiendo archivo ya procesado: {key}")
                        continue
                    
                    logger.info(f"\n📄 Procesando: {key}")
                    
                    # Descargar archivo temporal
                    local_path = os.path.join(temp_dir, key.replace('/', '_'))
                    self.download_object(key, local_path)
                    
                    # Cargar a BigQuery
                    if self._load_jsonl_to_bigquery(local_path, key):
                        self.stats['processed_files'] += 1
                        self.stats['processed_keys'].add(key)
                    else:
                        self.stats['failed_files'] += 1
                    
                    # Eliminar archivo temporal
                    os.remove(local_path)
                    
                    # Guardar checkpoint periódicamente
                    processed_files_checkpoint += 1
                    if processed_files_checkpoint >= save_checkpoint_interval:
                        self._save_checkpoint()
                        processed_files_checkpoint = 0
                
                # Actualizar token de continuación para el checkpoint
                self.stats['last_continuation_token'] = response.get('NextContinuationToken')
                
                # Verificar si hay más páginas
                if response.get('IsTruncated', False):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break
                
            except Exception as e:
                logger.error(f"❌ Error durante el procesamiento: {str(e)}")
                logger.info("🔄 Guardando checkpoint y reintentando en 30 segundos...")
                self._save_checkpoint()
                time.sleep(30)
                # Recargar checkpoint y continuar
                self._load_checkpoint()
                continuation_token = self.stats.get('last_continuation_token')
        
        # Procesamiento completado
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        logger.info(f"\n📊 Procesamiento completo en {duration}:")
        logger.info(f"   ✅ Archivos procesados exitosamente: {self.stats['processed_files']}")
        logger.info(f"   ❌ Archivos con errores: {self.stats['failed_files']}")
        logger.info(f"   📝 Líneas JSON válidas procesadas: {self.stats['processed_lines']}")
        logger.info(f"   ⚠️ Líneas JSON inválidas omitidas: {self.stats['invalid_lines']}")
        
        # Eliminar checkpoint una vez que se completa el proceso
        self._clear_checkpoint()
    
    def process_single_file(self, key, temp_dir="./temp_files"):
        """Procesa un único archivo JSONL del bucket"""
        # Crear directorio temporal si no existe
        os.makedirs(temp_dir, exist_ok=True)
        
        if not self._is_jsonl_file(key):
            logger.error(f"❌ El archivo {key} no es un archivo JSONL válido")
            return False
        
        logger.info(f"🔍 Procesando archivo individual: {key}")
        
        # Descargar archivo temporal
        local_path = os.path.join(temp_dir, key.replace('/', '_'))
        self.download_object(key, local_path)
        
        # Cargar a BigQuery
        result = self._load_jsonl_to_bigquery(local_path, key)
        
        # Eliminar archivo temporal
        os.remove(local_path)
        
        if result:
            logger.info(f"✅ Archivo {key} procesado exitosamente")
        else:
            logger.error(f"❌ Error al procesar archivo {key}")
        
        return result
    
    def cleanup(self, temp_dir="./temp_files"):
        """Limpia los archivos temporales"""
        if os.path.exists(temp_dir):
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(temp_dir)
            logger.info(f"🧹 Directorio temporal {temp_dir} eliminado")
    
    def get_processing_stats(self):
        """Retorna las estadísticas de procesamiento actuales"""
        return self.stats

if __name__ == "__main__":
    # Ruta al archivo de credenciales de Google Cloud
    credentials_path = "credentials/bubbo-dfba0-47e395cdcdc7.json"  # Reemplaza con tu ruta real
    
    # Iniciar el proceso de transferencia
    transfer = DOToBigQueryTransfer(credentials_path)
    try:
        # Para probar un archivo específico (recomendado para pruebas iniciales)
        # transfer.process_single_file("ComingSoon/latest/comingsoon.jsonl")
        
        # O procesar todo el bucket o un prefijo específico
        # Guardar checkpoint cada 5 archivos procesados
        transfer.process_bucket(save_checkpoint_interval=5)
    except KeyboardInterrupt:
        logger.warning("🛑 Proceso interrumpido por el usuario. Guardando checkpoint...")
        transfer._save_checkpoint()
    finally:
        transfer.cleanup()
