import random
import pandas as pd
from sqlalchemy import text
import unicodedata
from sqlalchemy.ext.asyncio import AsyncSession
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

def limpiar_nombre(nombre):
    if not nombre: return "0"
    n = str(nombre).lower().strip()
    return "".join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

# ... (Mantén tu MAPA_ANIMALES y limpiar_nombre igual) ...

async def generar_prediccion(db: AsyncSession):
    try:
        from datetime import datetime
        import pytz
        
        # Ajuste de hora operativa (Venezuela)
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p")
        hora_int = ahora.hour

        # A. CONSULTAR LA MEMORIA NEURAL (probabilidades_hora)
        # Aquí buscamos los 3 mejores según el entrenamiento de los 28k datos
        query = text("""
            SELECT animalito, probabilidad, tendencia 
            FROM probabilidades_hora 
            WHERE hora = :h 
            ORDER BY probabilidad DESC LIMIT 3
        """)
        res = await db.execute(query, {"h": hora_int})
        datos_ia = res.fetchall()

        if not datos_ia:
            return {"error": "Cerebro no entrenado. Por favor, pulsa RE-CALIBRAR."}

        top3 = []
        for i, r in enumerate(datos_ia):
            name = r[0].lower()
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
            
            # Buscamos la etiqueta Frío/Caliente para el Dashboard
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2] # CALIENTE o FRÍO
            })

        # B. REGISTRO EN AUDITORIA PARA MÉTRICAS DE VERDAD
        await db.execute(text("""
            INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
            VALUES (:f, :h, :a, :c, :p)
        """), {
            "f": ahora.date(), "h": hora_str, "a": top3[0]["animal"].lower(), 
            "c": float(top3[0]["porcentaje"].replace('%','')), "p": "Neural V4.5 PRO"
        })
        await db.commit()

        return {
            "decision": "ALTA PROBABILIDAD",
            "top3": top3,
            "analisis": f"Patrón detectado para las {hora_str}. Meta 5/11 activa."
        }
    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def analizar_estadisticas(db: AsyncSession):
    """Esta función es la que llena tu gráfica de Frecuencia Histórica"""
    query = text("SELECT animalito, COUNT(*) as c FROM historico GROUP BY 1 ORDER BY c DESC LIMIT 10")
    res = await db.execute(query)
    filas = res.fetchall()
    return {"status": "success", "data": {r[0].upper(): r[1] for r in filas}}
