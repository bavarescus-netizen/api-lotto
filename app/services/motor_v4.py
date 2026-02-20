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
        hora_str = ahora.strftime("%I:00 %p")

        # 1. Traer Top 3 desde la tabla de inteligencia
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

        if not datos_ia:
            return {"error": "Cerebro no entrenado. Ejecute Entrenamiento."}

        top3 = []
        for r in datos_ia:
            name = r[0].lower() if r[0] else "desconocido"
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "--")
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{num}.png", # Cambiado a número para consistencia
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2]
            })

        # 2. Guardar en auditoría si no existe para esta hora
        if top3:
            check_query = text("SELECT id FROM auditoria_ia WHERE fecha = :f AND hora = :h")
            check_res = await db.execute(check_query, {"f": ahora.date(), "h": hora_str})
            
            if not check_res.fetchone():
                await db.execute(text("""
                    INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                    VALUES (:f, :h, :a, :c, :p)
                """), {
                    "f": ahora.date(), 
                    "h": hora_str, 
                    "a": top3[0]["animal"].lower(),
                    "c": float(top3[0]["porcentaje"].replace('%','')), 
                    "p": f"Neural V4.5 PRO | Top1: {top3[0]['animal']}"
                })
                await db.commit()

        decision_final = "META 5/11 ACTIVA" if top3[0]["tendencia"] != "ROJO" else "⚠️ RIESGO ALTO - NO OPERAR"

        return {
            "decision": decision_final, 
            "top3": top3, 
            "analisis": f"Basado en 28,709 registros | Fecha: {ahora.date()} | Hora: {hora_str}"
        }
    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def obtener_bitacora_avance(db: AsyncSession):
    try:
        # 3. Traer bitácora con cruce de probabilidades reales para auditoría
        query = text("""
            SELECT a.hora, a.animal_predicho, a.resultado_real, a.acierto, a.confianza_pct,
                   p.probabilidad as prob_real_salio
            FROM auditoria_ia a
            LEFT JOIN probabilidades_hora p ON 
                (LOWER(a.resultado_real) = LOWER(p.animalito) AND CAST(SUBSTRING(a.hora, 1, 2) AS INTEGER) = p.hora)
            WHERE a.fecha = CURRENT_DATE
            ORDER BY a.hora DESC 
            LIMIT 8
        """)
        res = await db.execute(query)
        bitacora = []
        
        for r in res.fetchall():
            # Extraer número del animal real para la imagen
            animal_real = r[2] if r[2] else "PENDIENTE"
            num_real = "00"
            if animal_real != "PENDIENTE":
                # Buscar número en el MAPA
                num_real = next((k for k, v in MAPA_ANIMALES.items() if v.lower() in animal_real.lower()), "00")

            bitacora.append({
                "hora": r[0],
                "animal_predicho": r[1].upper() if r[1] else "---",
                "probabilidad": f"{r[4]}%" if r[4] else "N/A",
                "resultado_real": animal_real.upper(),
                "acierto": r[3],
                "img_real": f"{num_real}.png",
                "prob_real": f"{round(r[5], 1)}%" if r[5] else "2.1%" 
            })
        return bitacora
    except Exception as e:
        print(f"Error en bitácora: {e}")
        return []

async def examen_cerebro(db: AsyncSession):
    """Calcula la efectividad real desde el 7 de febrero"""
    try:
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN acierto = TRUE THEN 1 ELSE 0 END) as ganadas,
                AVG(CASE WHEN acierto IS NOT NULL THEN (CASE WHEN acierto THEN 100 ELSE 0 END) ELSE NULL END) as efectividad
            FROM auditoria_ia
            WHERE fecha >= '2026-02-07'
        """)
        res = await db.execute(query)
        stats = res.fetchone()
        
        return {
            "periodo": "7 Feb al Hoy",
            "total_jugadas": stats[0] if stats[0] else 0,
            "ganadas": stats[1] if stats[1] else 0,
            "porcentaje_exito": f"{round(stats[2], 2) if stats[2] else 0}%"
        }
    except Exception as e:
        return {"error": str(e)}
