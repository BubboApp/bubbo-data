
import os
from google.cloud import bigquery

# Establecer credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/python-13/firebase.json"

# Configurar cliente
client = bigquery.Client(location="europe-southwest1")
dataset_id = "bubbo-dfba0.bbmedia"
dataset_id_destiny = "bubbo-dfba0.content"

# Obtener todas las tablas dentro del dataset
tables = list(client.list_tables(dataset_id))
table_names = [table.table_id for table in tables]

# Limite de BigQuery: 1000 tablas por consulta
MAX_TABLES_PER_QUERY = 900

# Crear tabla vacía si no existe
client.query(f"""
 CREATE TABLE IF NOT EXISTS `{dataset_id_destiny}.unified_content`
 (
   tmdb_id STRING,
   genres STRING,
   title STRING,
   synopsis STRING,
   source_table STRING
 )
""").result()
print("Tabla destino creada o verificada.")

def process_tables_in_batches(table_names, batch_size):
    for i in range(0, len(table_names), batch_size):
        batch = table_names[i:i + batch_size]
        union_queries = [
            f"""
            SELECT 
              CAST(tmdb_id AS STRING) AS tmdb_id, 
              CAST(genres AS STRING) AS genres, 
              CAST(title AS STRING) AS title, 
              CAST(synopsis AS STRING) AS synopsis, 
              '{table}' AS source_table 
            FROM `{dataset_id}.{table}`
            """
            for table in batch
        ]
        
        batch_query = f"""
        INSERT INTO `{dataset_id_destiny}.unified_content`
        WITH all_data AS (
            {' UNION ALL '.join(union_queries)}
        )
        SELECT DISTINCT tmdb_id, genres, title, synopsis, source_table
        FROM all_data
        WHERE tmdb_id IS NOT NULL;
        """
        
        try:
            job = client.query(batch_query)
            job.result()  # Esperar a que termine la consulta
            print(f"Procesado batch {i//batch_size + 1}: {len(batch)} tablas")
        except Exception as e:
            print(f"Error en el batch {i//batch_size + 1}: {str(e)}")
            # Si necesitas más detalles del error, puedes dividir el batch en partes más pequeñas
            # o procesar las tablas individualmente para identificar la problemática

# Procesar las tablas en lotes
process_tables_in_batches(table_names, MAX_TABLES_PER_QUERY)
print(f"Tabla 'unified_content' creada exitosamente con datos de {len(table_names)} tablas.")

# Crear tabla 'best_content'
create_best_query = f"""
CREATE OR REPLACE TABLE `{dataset_id_destiny}.best_content` AS
WITH ranked AS (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY tmdb_id
  ORDER BY LENGTH(synopsis) DESC, LENGTH(title) DESC, LENGTH(genres) DESC) AS rank
  FROM `{dataset_id_destiny}.unified_content`
)
SELECT tmdb_id, genres, title, synopsis
FROM ranked
WHERE rank = 1 AND tmdb_id IS NOT NULL;
"""

client.query(create_best_query).result()
print("Tabla 'best_content' creada exitosamente con los mejores IDs.")