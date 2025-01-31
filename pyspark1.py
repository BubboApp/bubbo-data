import json
import logging
import pymongo
import boto3
from botocore.client import Config
from datetime import datetime
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration updated for AWS DocumentDB cluster
CONFIG = {
    'do_spaces': {
        'access_key': 'DO00WC26X8H2CFGZCWC8',
        'secret_key': 'fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc',
        'endpoint': 'https://nyc3.digitaloceanspaces.com',
        'bucket': 'bb-bubbo',
        'prefix': 'Content/latest/'
    },
    'mongodb': {
        'host': 'mongodb-cluster-test.cluster-c5npejalcudh.eu-west-3.docdb.amazonaws.com:27017',
        'username': 'bubbo',
        'password': 'bubbomaster',
        'ssl': True,
        'ssl_ca_certs': 'global-bundle.pem',
        'authSource': 'admin',
        'database': 'bubbo',
        'collection': 'ID_Deeplinks'
    }
}

# Spark session initialization
spark = SparkSession.builder \
    .appName("DO Spaces File Processing") \
    .getOrCreate()

@contextmanager
def get_mongo_collection(config):
    """Context manager to get MongoDB collection with SSL support."""
    mongo_client = None
    try:
        mongo_client = pymongo.MongoClient(
            host=config['host'],
            username=config['username'],
            password=config['password'],
            ssl=config.get('ssl', False),
            ssl_ca_certs=config.get('ssl_ca_certs'),
            authSource=config['authSource']
        )
        db = mongo_client[config['database']]
        collection = db[config['collection']]
        yield collection
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        yield None
    finally:
        if mongo_client:
            mongo_client.close()

# Rest of the script remains identical to the previous version...

@udf(StringType())
def extract_numeric_id(line):
    """Extract first numeric ID from ExternalIds."""
    try:
        data = json.loads(line)
        external_ids = data.get("ExternalIds", [])

        if not external_ids or not isinstance(external_ids, list):
            return None

        for item in external_ids:
            if isinstance(item, dict) and "ID" in item:
                id_value = item["ID"]
                if isinstance(id_value, int):
                    return str(id_value)
                elif isinstance(id_value, str) and id_value.isdigit():
                    return str(id_value)
        return None
    except Exception as e:
        logger.error(f"ID extraction error: {e}")
        return None

@udf(StringType())
def extract_deeplinks(line):
    """Extract Deeplinks from given line."""
    try:
        data = json.loads(line)
        deeplinks = data.get("Deeplinks", {})
        return json.dumps(deeplinks) if isinstance(deeplinks, dict) else "{}"
    except Exception as e:
        logger.error(f"Deeplink extraction error: {e}")
        return "{}"

@udf(StringType())
def extract_additional_fields(line):
    """Extract additional fields from the line, including Title, CleanTitle, OriginalTitle, Type, Year, Duration."""
    try:
        data = json.loads(line)

        additional_fields = {
            "Title": data.get("Title", ""),
            "CleanTitle": data.get("CleanTitle", ""),
            "OriginalTitle": data.get("OriginalTitle", None),
            "Type": data.get("Type", ""),
            "Year": data.get("Year", None),
            "Duration": data.get("Duration", None)
        }

        return json.dumps(additional_fields)

    except Exception as e:
        logger.error(f"Additional fields extraction error: {e}")
        return "{}"

def get_last_processed_file(collection):
    """Retrieve the last processed file from MongoDB."""
    last_file = collection.find_one(sort=[("processed_at", pymongo.DESCENDING)])
    return last_file['file_key'] if last_file else None

def process_do_spaces_files(starting_file=None):
    """Process DigitalOcean Spaces files and store in MongoDB using PySpark."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=CONFIG['do_spaces']['access_key'],
        aws_secret_access_key=CONFIG['do_spaces']['secret_key'],
        endpoint_url=CONFIG['do_spaces']['endpoint'],
        config=Config(signature_version='s3v4'),
        region_name='nyc3'
    )

    # Processing metrics
    metrics = {
        'total_files': 0,
        'processed_files': 0,
        'total_lines': 0,
        'inserted_lines': 0,
        'error_files': []
    }

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=CONFIG['do_spaces']['bucket'],
            Prefix=CONFIG['do_spaces']['prefix']
        )

        with get_mongo_collection(CONFIG['mongodb']) as collection:
            if collection is None:
                logger.error("Could not get MongoDB collection. Aborting processing.")
                return

            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    metrics['total_files'] += 1

                    # Check if we should skip to the last processed file
                    if starting_file and key != starting_file:
                        continue

                    try:
                        # Read the file content into an RDD
                        file_obj = s3_client.get_object(
                            Bucket=CONFIG['do_spaces']['bucket'],
                            Key=key
                        )

                        file_content = file_obj['Body'].read().decode('utf-8')
                        lines = file_content.splitlines()

                        # Convert lines into an RDD and then a DataFrame
                        lines_rdd = spark.sparkContext.parallelize(lines)
                        df = lines_rdd.map(lambda line: (line, )).toDF(["line"])

                        # Apply UDFs to extract data
                        df = df.withColumn("extracted_id", extract_numeric_id(df["line"]))
                        df = df.withColumn("deeplinks", extract_deeplinks(df["line"]))
                        df = df.withColumn("additional_fields", extract_additional_fields(df["line"]))

                        # Show the first few rows for logging
                        df.show(5)

                        # Collect the results and insert them into MongoDB
                        for row in df.collect():
                            line = row['line']
                            extracted_id = row['extracted_id']
                            deeplinks = row['deeplinks']
                            additional_fields = row['additional_fields']

                            if extracted_id and deeplinks:
                                try:
                                    doc = {
                                        'file_key': key,
                                        'extracted_id': extracted_id,
                                        'content': json.loads(line),
                                        'deeplinks': json.loads(deeplinks),
                                        'additional_fields': json.loads(additional_fields),
                                        'processed_at': datetime.utcnow()
                                    }

                                    existing_doc = collection.find_one({
                                        'file_key': key,
                                        'extracted_id': extracted_id
                                    })

                                    if existing_doc:
                                        existing_timestamp = existing_doc.get('processed_at')
                                        if existing_timestamp and doc['processed_at'] <= existing_timestamp:
                                            logger.info(f"Skipping update: {key}")
                                            continue

                                    result = collection.update_one(
                                        {'file_key': key, 'extracted_id': extracted_id},
                                        {'$set': doc},
                                        upsert=True
                                    )

                                    if result.upserted_id:
                                        logger.info(f"New Document Inserted: {key}")
                                    elif result.modified_count > 0:
                                        logger.info(f"Document Updated: {key}")

                                    metrics['inserted_lines'] += 1

                                except Exception as insert_error:
                                    logger.error(f"MongoDB insertion error: {insert_error}")

                        metrics['processed_files'] += 1

                        # Update the last processed file in MongoDB
                        collection.update_one(
                            {'file_key': key},
                            {'$set': {'processed_at': datetime.utcnow()}},
                            upsert=True
                        )

                    except Exception as e:
                        logger.error(f"File processing error {key}: {e}")
                        metrics['error_files'].append(key)

    except Exception as e:
        logger.error(f"Processing error: {e}")
    finally:
        # Final processing summary
        logger.info(
            f"Processing Summary:\n"
            f"Total files: {metrics['total_files']}\n"
            f"Processed files: {metrics['processed_files']}\n"
            f"Total lines: {metrics['total_lines']}\n"
            f"Inserted lines: {metrics['inserted_lines']}"
        )

        if metrics['error_files']:
            logger.warning("Files with errors:")
            for error_file in metrics['error_files']:
                logger.warning(f"  {error_file}")

def main():
    with get_mongo_collection(CONFIG['mongodb']) as collection:
        if collection is None:
            logger.error("Could not get MongoDB collection. Aborting.")
            return

        last_processed_file = get_last_processed_file(collection)
        if last_processed_file:
            user_choice = input(f"Last processed file was '{last_processed_file}'. Do you want to continue from this file? (yes/no): ")
            if user_choice.lower() == 'yes':
                process_do_spaces_files(starting_file=last_processed_file)
            else:
                process_do_spaces_files()
        else:
            process_do_spaces_files()

if __name__ == "__main__":
    main()