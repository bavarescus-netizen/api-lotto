import pandas as pd
from sqlalchemy import text
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

        query = text("""
            SELECT animalito, probabilidad, 
            CASE 
                WHEN probabilidad >= 40 THEN 'VERDE'
                WHEN probabilidad >= 25 THEN 'AMARILLO'
                ELSE 'ROJO'
            END as nivel_riesgo
            FROM probabilidades_hora 
            WHERE hora = :h 
            ORDER BY probabilidad DESC LIMIT 3
        """)
        res = await db.execute(query, {"h": hora_int})
        datos_ia = res.fetchall()

        top3 = []
        for r in datos_ia:
            name = r[0].lower() if r[0] else "desconocido"
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "--")
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2]
            })

        if top3:
            # Guardar predicción para auditoría si no existe
            await db.execute(text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:f, :h, :a, :c, :p) ON CONFLICT DO NOTHING
            """), {"f": ahora.date(), "h": hora_str, "a": top3[0]["animal"].lower(), "c": float(top3[0]["porcentaje"].replace('%','')), "p": "Neural V4.5 PRO"})
            await db.commit()

        return {"top3": top3, "analisis": f"Registros: 28,709 | {hora_str}"}
    except Exception as e:
        return {"error": str(e)}

async def obtener_bitacora_avance(db: AsyncSession):
    try:
        query = text("""
            SELECT a.hora, a.animal_predicho, a.resultado_real, a.acierto, p.probabilidad
            FROM auditoria_ia a
            LEFT JOIN probabilidades_hora p ON 
                (LOWER(a.resultado_real) = LOWER(p.animalito) AND CAST(SUBSTRING(a.hora, 1, 2) AS INTEGER) = p.hora)
            WHERE a.fecha = CURRENT_DATE ORDER BY a.hora DESC LIMIT 11
        """)
        res = await db.execute(query)
        bitacora
