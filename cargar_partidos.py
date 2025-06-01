import pandas as pd
from pymongo import MongoClient

# 1. Conexión a MongoDB local
client = MongoClient("mongodb://localhost:27017/")
db = client["tenis"]  # Base de datos: tenis
collection = db["partidos"]  # Colección: partidos

# 2. Cargar archivos Excel
archivos = ["data/2022.xlsx", "data/2023.xlsx"]

# 3. Leer y guardar en MongoDB
for archivo in archivos:
    print(f"Procesando {archivo}...")
    df = pd.read_excel(archivo)  # Leer archivo Excel con pandas
    data = df.to_dict(orient="records")  # Convertir DataFrame a lista de diccionarios
    collection.insert_many(data)  # Insertar documentos en MongoDB

print("Datos insertados correctamente en MongoDB.")

#  4. Consultas en MongoDB

# Consulta 1: Cantidad de partidos por año
print("1ERA Consulta.")
print("\nCantidad de partidos por año:")
partidos_por_anio = collection.aggregate([
    {
        # Extraer el año del campo "Date" (asumiendo formato dd-mm-yyyy)
        "$project": {
            "anio": {"$substr": ["$Date", 6, 4]}
        }
    },
    {
        # Agrupar por año y contar la cantidad de documentos (partidos)
        "$group": {
            "_id": "$anio",
            "total": {"$sum": 1}
        }
    }
])
for resultado in partidos_por_anio:
    print(f"Año {resultado['_id']}: {resultado['total']} partidos")

# Consulta 2: Jugadores con más partidos ganados
print("2da Consulta.")
print("\nTop 5 jugadores con más partidos ganados:")
ganadores = collection.aggregate([
    # Agrupar por campo "Winner" y contar victorias
    {"$group": {"_id": "$Winner", "ganados": {"$sum": 1}}},
    # Ordenar descendente por cantidad de partidos ganados
    {"$sort": {"ganados": -1}},
    # Limitar a los 5 primeros resultados
    {"$limit": 5}
])
for jugador in ganadores:
    print(f"{jugador['_id']}: {jugador['ganados']} partidos ganados")
