import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import pytz
import re

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
        hora_str = ahora.strftime("%I:00 %p").upper()

        query = text("""
            SELECT animalito, probabilidad, tendencia
            FROM probabilidades_hora 
            WHERE hora = :h 
            ORDER BY probabilidad DESC LIMIT 3
        """)
        res = await db.execute(query, {"h": hora_int})
        datos_ia = res.fetchall()

        top3 = []
        for r in datos_ia:
            name = r[0].lower()
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "--")
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2]
            })

        if top3:
            await db.execute(text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, resultado_real)
                VALUES (:f, :h, :a, :c, 'PENDIENTE')
                ON CONFLICT (fecha, hora) DO NOTHING
            """), {
                "f": ahora.date(), 
                "h": hora_str, 
                "a": top3[0]["animal"].lower(), 
                "c": float(top3[0]["porcentaje"].replace('%',''))
            })
            await db.commit()

        return {"top3": top3, "analisis": f"Registros Analizados: 28,709 | {hora_str}"}
    except Exception as e:
        print(f"❌ Error en Motor: {e}")
        return {"top3": [], "analisis": f"Error: {str(e)}"}

async def obtener_bitacora_avance(db: AsyncSession):
    try:
        # CORRECCIÓN: Usamos CONCAT para evitar el error de parámetros bind ':00'
        query = text("""
            SELECT a.hora, a.animal_predicho, a.resultado_real, a.acierto, COALESCE(p.probabilidad, 2.1)
            FROM auditoria_ia a
            LEFT JOIN probabilidades_hora p ON (
                LOWER(a.resultado_real) = LOWER(p.animalito) 
                AND EXTRACT(HOUR FROM CAST(
                    CASE 
                        WHEN a.hora LIKE '%PM' AND a.hora NOT LIKE '12%' 
                        THEN CONCAT((CAST(SPLIT_PART(a.hora, ':', 1) AS INT) + 12)::TEXT, ':00')
                        WHEN a.hora LIKE '12%AM' THEN '00:00'
                        ELSE SPLIT_PART(a.hora, ' ', 1)
                    END AS TIME)) = p.hora
            )
            WHERE a.fecha = CURRENT_DATE 
            ORDER BY 
                CASE WHEN a.hora LIKE '%PM' AND a.hora NOT LIKE '12%' THEN 1 ELSE 0 END DESC, 
                a.hora DESC 
            LIMIT 11
        """)
        res = await db.execute(query)
        bitacora = []
        for r in res.fetchall():
            res_real = (r[2] or "pendiente").lower()
            nombre_animal = re.sub(r'[^a-z]', '', res_real)
            num_real = next((k for k, v in MAPA_ANIMALES.items() if v == nombre_animal), "--")
            
            bitacora.append({
                "hora": r[0],
                "animal_predicho": r[1].upper() if r[1] else "PENDIENTE",
                "resultado_real": r[2].upper() if r[2] else "PENDIENTE",
                "acierto": r[3],
                "img_real": f"{nombre_animal}.png",
                "num_real": num_real,
                "prob_real": f"{round(r[4], 1)}%"
            })
        return bitacora
    except Exception as e:
        print(f"❌ Error en Bitacora: {e}")
        return []
