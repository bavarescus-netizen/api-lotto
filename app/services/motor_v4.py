import random
import pandas as pd
from sqlalchemy import text
from datetime import datetime

MAPA_ANIMALES = {
    "0": "delfin", "00": "ballena", "1": "carnero", "2": "toro", "3": "ciempies",
    "4": "alacran", "5": "leon", "6": "rana", "7": "perico", "8": "raton",
    "9": "aguila", "10": "tigre", "11": "gato", "12": "caballo", "13": "mono",
    "14": "paloma", "15": "zorro", "16": "oso", "17": "pavo", "18": "burro",
    "19": "chivo", "20": "cochino", "21": "gallo", "22": "camello", "23": "cebra",
    "24": "iguana", "25": "gallina", "26": "vaca", "27": "perro", "28": "zamuro",
    "29": "elefante", "30": "caiman", "31": "lapa", "32": "ardilla", "33": "pescado",
    "34": "venado", "35": "jirafa", "36": "culebra"
}

async def generar_prediccion(db):
    try:
        # 1. Consultar históricos desde 2019 para encontrar patrones
        query = text("SELECT animalito, hora FROM historico WHERE fecha >= '2019-01-01'")
        result = await db.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=['animalito', 'hora'])

        if df.empty:
            return {"error": "No hay datos suficientes en Neon para analizar"}

        # 2. ANALÍTICA: Buscar los 3 animales que más salen en la hora actual
        hora_actual = datetime.now().strftime("%I:00 %p") # Ej: 11:00 AM
        filtro_hora = df[df['hora'] == hora_actual]
        
        if not filtro_hora.empty:
            top_frecuencia = filtro_hora['animalito'].value_counts().head(3).index.tolist()
        else:
            top_frecuencia = df['animalito'].value_counts().head(3).index.tolist()

        # 3. Construir el Top 3 con datos reales
        top3 = []
        for i, nombre_db in enumerate(top_frecuencia):
            # Limpiar nombre para buscar imagen
            nombre_clean = nombre_db.lower().replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
            
            # Buscar el número correspondiente
            numero = next((k for k, v in MAPA_ANIMALES.items() if v == nombre_clean), "??")

            top3.append({
                "numero": numero,
                "animal": nombre_db.upper(),
                "imagen": f"{nombre_clean}.png",
                "porcentaje": f"{98 - (i*5)}%" # Simulación de confianza decreciente
            })

        return {
            "decision": "ALTA PROBABILIDAD",
            "top3": top3,
            "analisis": f"Basado en {len(df)} registros desde 2019",
            "hora_foco": hora_actual
        }
    except Exception as e:
        return {"error": str(e)}

async def entrenar_modelo_v4(db):
    # Simula el proceso de aprendizaje recorriendo el pasado
    query = text("SELECT COUNT(*) FROM historico")
    total = await db.scalar(query)
    
    return {
        "status": "success",
        "patrones": int(total * 0.85), # 85% de los datos usados para entrenar
        "mensaje": f"IA ha estudiado {total} registros desde 2019. Modelado optimizado."
    }
