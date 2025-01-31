import boto3
import json
import math

# Credenciales de AWS
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION_NAME = ''
AWS_BUCKET_NAME = ''
TARGET_PREFIX = ''  # Ruta específica en el bucket


def list_jsonl_files(s3_client, bucket_name, prefix):
    """
    Lista los archivos JSONL en una carpeta específica.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No se encontraron archivos en la ruta {prefix}")
            return []
        
        # Filtrar solo los archivos JSONL
        return [file['Key'] for file in response['Contents'] if file['Key'].endswith('.jsonl')]
    except Exception as e:
        print(f"Error al listar archivos: {e}")
        return []


def get_jsonl_from_s3(s3_client, bucket_name, key):
    """
    Obtiene el contenido de un archivo JSONL y lo devuelve como una lista de objetos JSON.
    """
    try:
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        file_content = file_obj['Body'].read().decode('utf-8')
        json_lines = []
        
        for line in file_content.strip().split('\n'):
            try:
                json_lines.append(json.loads(line))  # Convertir cada línea a JSON
            except json.JSONDecodeError as e:
                print(f"Error al decodificar una línea en {key}: {e}")
        
        return json_lines
    except Exception as e:
        print(f"Error al obtener el archivo {key}: {e}")
        return None


def display_files_with_pagination(files, page_size=10):
    """
    Muestra una lista de archivos con paginación.
    """
    total_pages = math.ceil(len(files) / page_size)
    current_page = 1

    while True:
        # Calcular los índices para la paginación
        start_index = (current_page - 1) * page_size
        end_index = start_index + page_size
        page_files = files[start_index:end_index]

        # Mostrar archivos de la página actual
        print(f"\nPágina {current_page}/{total_pages}:")
        for i, file in enumerate(page_files, start=start_index + 1):
            print(f"{i}. {file}")

        # Navegación
        if total_pages > 1:
            print("\nOpciones:")
            if current_page > 1:
                print("p: Página anterior")
            if current_page < total_pages:
                print("n: Página siguiente")
            print("q: Salir")

        # Pedir entrada al usuario
        user_input = input("\nSelecciona un número de archivo o una opción: ").strip().lower()
        if user_input.isdigit():
            file_index = int(user_input)
            if 1 <= file_index <= len(files):
                return files[file_index - 1]
            else:
                print("Número fuera de rango. Intenta de nuevo.")
        elif user_input == 'p' and current_page > 1:
            current_page -= 1
        elif user_input == 'n' and current_page < total_pages:
            current_page += 1
        elif user_input == 'q':
            return None
        else:
            print("Entrada no válida. Intenta de nuevo.")


def main():
    # Inicializar cliente S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME
    )

    # Listar archivos JSONL en la carpeta 'Content/latest/'
    jsonl_files = list_jsonl_files(s3_client, AWS_BUCKET_NAME, TARGET_PREFIX)
    if not jsonl_files:
        print("No se encontraron archivos JSONL en la carpeta especificada.")
        return

    # Mostrar archivos con paginación y permitir selección
    selected_file = display_files_with_pagination(jsonl_files)
    if not selected_file:
        print("No se seleccionó ningún archivo. Saliendo...")
        return

    print(f"\nHas seleccionado el archivo: {selected_file}")

    # Descargar y procesar el archivo JSONL
    jsonl_content = get_jsonl_from_s3(s3_client, AWS_BUCKET_NAME, selected_file)
    if jsonl_content:
        print(f"\nContenido del archivo {selected_file}:")
        for item in jsonl_content:
            print(json.dumps(item, indent=2))  # Mostrar contenido formateado
    else:
        print("No se pudo obtener el contenido del archivo.")


if __name__ == "__main__":
    main()
