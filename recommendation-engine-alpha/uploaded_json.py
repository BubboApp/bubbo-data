import firebase_admin
from firebase_admin import credentials, firestore
import json
from tqdm import tqdm  # Importar tqdm para la barra de progreso

if not firebase_admin._apps:
    cred = credentials.Certificate(r'C:\Users\Carlo\Desktop\Proyectos\data\bubbo-data\test\bubbo-dfba0-firebase-adminsdk-fbsvc-79dc4511e7 (5).json')
    firebase_admin.initialize_app(cred)

db = firestore.client()

json_file_path = r'C:\Users\Carlo\Desktop\Proyectos\data\bubbo-data\test\extracted_data.json'

# Cargar el archivo JSON
with open(json_file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

# Verificar si los datos son una lista o un diccionario
if isinstance(data, list):
    # Usar tqdm para mostrar la barra de progreso
    for movie in tqdm(data, desc="Agregando documentos", unit="película", ncols=100):
        # Usar el valor de la columna "ID" como el ID del documento
        movie_id = str(movie['ID'])
        db.collection('Data_Clean').document(movie_id).set(movie)
else:
    print("Error: El JSON debe ser una lista.")