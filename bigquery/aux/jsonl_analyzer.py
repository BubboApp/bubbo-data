import boto3
import json

class DOToBigQueryTransfer:
    def __init__(self):
        self.session = boto3.session.Session()
        self.s3_client = self.session.client(
            's3',
            region_name="nyc3",
            endpoint_url="https://nyc3.digitaloceanspaces.com",
            aws_access_key_id="DO00WC26X8H2CFGZCWC8",
            aws_secret_access_key="fR+CZU0H4ErMYIuZOzqpsWlzxu5tvCW0Iokx1v0nZfc"
        )
        self.bucket_name = "bb-bubbo"

    def list_subdirectories(self):
        """Lista los subdirectorios en el bucket"""
        subdirs = set()
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Delimiter='/')
        for prefix in response.get("CommonPrefixes", []):
            subdirs.add(prefix["Prefix"])
        return subdirs

    def get_jsonl_structure(self, file_key):
        """Obtiene la estructura de un archivo JSONL sin cargar todo el contenido"""
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        first_line = response["Body"].readline().decode("utf-8")
        
        try:
            json_obj = json.loads(first_line)
            structure = {key: type(value).__name__ for key, value in json_obj.items()}
            return structure
        except json.JSONDecodeError:
            return None

    def extract_jsonl_structures(self):
        """Extrae la estructura JSONL de un archivo en cada subdirectorio"""
        subdirs = self.list_subdirectories()
        structures = {}

        for subdir in subdirs:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=subdir, MaxKeys=10)
            for obj in response.get("Contents", []):
                file_key = obj["Key"]
                if file_key.endswith(".jsonl"):
                    structures[subdir] = self.get_jsonl_structure(file_key)
                    break  # Solo procesamos un archivo por subdirectorio

        return structures


# Uso
if __name__ == "__main__":
    do_transfer = DOToBigQueryTransfer()
    jsonl_structures = do_transfer.extract_jsonl_structures()

    print(json.dumps(jsonl_structures, indent=2))
