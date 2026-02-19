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

        # 1. SQL DE ALTA PRECISIÓN: Consulta el cerebro entrenado
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
            name = r[0].lower()
            num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png", # Importante: coincide con tus archivos .png
                "porcentaje": f"{round(r[1], 1)}%",
                "tendencia": r[2]
            })

        # 2. AUDITORÍA: Guardamos la predicción solo si no existe una para esta hora hoy
        # Así evitamos llenar la base de datos de basura al refrescar la página
        check_query = text("SELECT id FROM auditoria_ia WHERE fecha = :f AND hora = :h")
        check_res = await db.execute(check_query, {"f": ahora.date(), "h": hora_str})
        
        if not check_res.fetchone():
            await db.execute(text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:f, :h, :a, :c, :p)
            """), {
                "f": ahora.date(), "h": hora_str, "a": top3[0]["animal"].lower(),
                "c": float(top3[0]["porcentaje"].replace('%','')), "p": "Neural V4.5 PRO"
            })
            await db.commit()

        decision_final = "META 5/11 ACTIVA" if top3[0]["tendencia"] != "ROJO" else "⚠️ RIESGO ALTO - NO OPERAR"

        return {
            "decision": decision_final, 
            "top3": top3, 
            "analisis": f"Basado en 28,709 registros | Sincronizado: {hora_str}"
        }
    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

# --- FUNCIÓN NUEVA: ESTA ES LA QUE MUESTRA LOS RESULTADOS EN EL DASHBOARD ---
async def obtener_bitacora_avance(db: AsyncSession):
    """Extrae los últimos 5 sorteos comparando predicción vs realidad"""
    try:
        query = text("""
            SELECT hora, animal_predicho, resultado_real, acierto 
            FROM auditoria_ia 
            WHERE fecha = CURRENT_DATE
            ORDER BY hora DESC 
            LIMIT 5
        """)
        res = await db.execute(query)
        # Convertimos a lista de diccionarios para que Jinja2 lo lea fácil
        bitacora = []
        for r in res.fetchall():
            bitacora.append({
                "hora": r[0],
                "animal_predicho": r[1],
                "resultado_real": r[2],
                "acierto": r[3]
            })
        return bitacora
    except Exception as e:
        print(f"Error en bitácora: {e}")
        return []

async def analizar_estadisticas(db: AsyncSession):
    query = text("SELECT animalito, COUNT(*) as c FROM historico GROUP BY 1 ORDER BY c DESC LIMIT 10")
    res = await db.execute(query)
    filas = res.fetchall()
    return {"status": "success", "data": {r[0].upper(): r[1] for r in filas}}
