import random
import pandas as pd
from sqlalchemy import text
import unicodedata
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import pytz

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

async def generar_prediccion(db: AsyncSession):
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_int = ahora.hour
        hora_str = ahora.strftime("%I:00 %p")

        # 1. Leer de la tabla de inteligencia (ya entrenada con 28k datos)
        query = text("""
            SELECT animalito, probabilidad, tendencia 
            FROM probabilidades_hora 
            WHERE hora = :h 
            ORDER BY probabilidad DESC LIMIT 3
        """)
        res = await db.execute(query, {"h": hora_int})
        datos_ia = res.fetchall()

        if not datos_ia:
            return {"error": "Cerebro no entrenado. Ejecute Re-Calibrar."}

        top3 = []
        for r in datos_ia:
            name = r[0].lower()
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2] # Caliente o Frío
            })

        # 2. Registrar en Auditoría para medir efectividad
        await db.execute(text("""
            INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
            VALUES (:f, :h, :a, :c, :p)
        """), {
            "f": ahora.date(), "h": hora_str, "a": top3[0]["animal"].lower(),
            "c": float(top3[0]["porcentaje"].replace('%','')), "p": "Neural V4.5 PRO"
        })
        await db.commit()

        return {"decision": "META 5/11 ACTIVA", "top3": top3, "analisis": f"Sincronizado: {hora_str}"}
    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def analizar_estadisticas(db: AsyncSession):
    """Genera la data para el gráfico de barras"""
    query = text("SELECT animalito, COUNT(*) as c FROM historico GROUP BY 1 ORDER BY c DESC LIMIT 10")
    res = await db.execute(query)
    filas = res.fetchall()
    return {"status": "success", "data": {r[0].upper(): r[1] for r in filas}}

async def entrenar_modelo_v4(db: AsyncSession):
    """Sincroniza aciertos entre predicción e historial"""
    query = text("""
        UPDATE auditoria_ia SET resultado_real = h.animalito,
        acierto = (LOWER(TRIM(auditoria_ia.animal_predicho)) = LOWER(TRIM(h.animalito)))
        FROM historico h WHERE auditoria_ia.fecha = h.fecha AND auditoria_ia.hora = h.hora AND auditoria_ia.acierto IS NULL
    """)
    res = await db.execute(query)
    return res.rowcount
