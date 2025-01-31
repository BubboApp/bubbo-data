import json
import logging
import pymongo
import boto3
from botocore.client import Config
from datetime import datetime
from contextlib import contextmanager

# Logging configuration remains the same
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration remains the same
CONFIG = {
    'do_spaces': {
        'access_key': '',
        'secret_key': '',
        'endpoint': '',
        'bucket': '',
        'prefix': ''
    },
    'mongodb': {
        'host': '192.168.1.115',
        'username': 'admin',
        'password': 'bubbomaster',
        'auth_source': 'admin',
        'database': 'bubbo',
        'collection': 'processed_files'
    }
}

# Existing context manager and helper functions remain the same
@contextmanager
def get_mongo_collection(config):
    """Context manager to get MongoDB collection."""
    mongo_client = None
    try:
        mongo_client = pymongo.MongoClient(
            host=config['host'],
            username=config['username'],
            password=config['password'],
            authSource=config['auth_source']
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

def extract_numeric_id(line):
    """Extract first numeric ID from ExternalIds with robust error handling."""
    try:
        data = json.loads(line)
        external_ids = data.get("ExternalIds", [])

        if not external_ids or not isinstance(external_ids, list):
            return None

        for item in external_ids:
            if isinstance(item, dict) and "ID" in item:
                id_value = item["ID"]
                if isinstance(id_value, int):
                    return id_value
                elif isinstance(id_value, str) and id_value.isdigit():
                    return int(id_value)
        return None

    except Exception as e:
        logger.error(f"ID extraction error: {e}")
        return None

def extract_deeplinks(line):
    """Extract Deeplinks from given line."""
    try:
        data = json.loads(line)
        deeplinks = data.get("Deeplinks", {})

        if isinstance(deeplinks, dict):
            return deeplinks
        else:
            logger.warning("Unexpected Deeplinks format, dictionary expected.")
            return {}
    except Exception as e:
        logger.error(f"Deeplink extraction error: {e}")
        return {}

def extract_additional_fields(line):
    """Extract additional fields from JSON line."""
    try:
        data = json.loads(line)
        return {
            'UID': data.get('UID'),
            'PlatformName': data.get('PlatformName'),
            'Title': data.get('Title'),
            'CleanTitle': data.get('CleanTitle'),
            'OriginalTitle': data.get('OriginalTitle'),
            'Type': data.get('Type'),
            'Year': data.get('Year'),
            'Duration': data.get('Duration')
        }
    except Exception as e:
        logger.error(f"Additional fields extraction error: {e}")
        return {}

def process_do_spaces_files():
    """Process DigitalOcean Spaces files and store in MongoDB."""
    # Initialize DO Spaces client
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
        # Paginate through S3 objects
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

                    try:
                        # Read and process file
                        file_obj = s3_client.get_object(
                            Bucket=CONFIG['do_spaces']['bucket'],
                            Key=key
                        )

                        file_content = file_obj['Body'].read().decode('utf-8')
                        lines = file_content.splitlines()

                        file_metrics = {
                            'processed_lines': 0,
                            'inserted_lines': 0
                        }

                        for line_num, line in enumerate(lines, 1):
                            if not line.strip():
                                continue

                            metrics['total_lines'] += 1
                            file_metrics['processed_lines'] += 1

                            extracted_id = extract_numeric_id(line)
                            deeplinks = extract_deeplinks(line)
                            additional_fields = extract_additional_fields(line)

                            if extracted_id and deeplinks:
                                try:
                                    doc = {
                                        'file_key': key,
                                        'line_number': line_num,
                                        'extracted_id': extracted_id,
                                        'content': json.loads(line),
                                        'deeplinks': deeplinks,
                                        'additional_fields': additional_fields,
                                        'processed_at': datetime.utcnow()
                                    }

                                    # Check if document exists
                                    existing_doc = collection.find_one({
                                        'file_key': key,
                                        'extracted_id': extracted_id
                                    })

                                    if existing_doc:
                                        # Compare timestamps
                                        existing_timestamp = existing_doc.get('processed_at')
                                        if existing_timestamp and doc['processed_at'] <= existing_timestamp:
                                            logger.info(
                                                f"⏳ Skipping update: {key}\n"
                                                f"   ID: {extracted_id}\n"
                                                f"   Existing timestamp: {existing_timestamp}\n"
                                                f"   New timestamp: {doc['processed_at']}"
                                            )
                                            continue

                                    # Insert or update document
                                    result = collection.update_one(
                                        {'file_key': key, 'extracted_id': extracted_id},
                                        {'$set': doc},
                                        upsert=True
                                    )

                                    # Log insertion/update
                                    if result.upserted_id:
                                        logger.info(
                                            f"✅ New Document Inserted: {key}\n"
                                            f"   ID: {extracted_id}\n"
                                            f"   Additional Fields: {additional_fields}"
                                        )
                                    elif result.modified_count > 0:
                                        logger.info(
                                            f"🔄 Document Updated: {key}\n"
                                            f"   ID: {extracted_id}\n"
                                            f"   Additional Fields: {additional_fields}"
                                        )

                                    metrics['inserted_lines'] += 1
                                    file_metrics['inserted_lines'] += 1

                                except Exception as insert_error:
                                    logger.error(f"MongoDB insertion error: {insert_error}")

                        # Log file processing results
                        logger.info(
                            f"File processed: {key}\n"
                            f"  Processed lines: {file_metrics['processed_lines']}\n"
                            f"  Inserted lines: {file_metrics['inserted_lines']}"
                        )

                        metrics['processed_files'] += 1

                    except Exception as e:
                        logger.error(f"File processing error {key}: {e}")
                        metrics['error_files'].append(key)

    except Exception as e:
        logger.error(f"Processing error: {e}")
    finally:
        # Final processing summary
        logger.info(
            "Processing Summary:\n"
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
    process_do_spaces_files()

if __name__ == "__main__":
    main()