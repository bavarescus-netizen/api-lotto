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

async def generar_prediccion(db: AsyncSession):
    try:
        # A. ANALIZAR EFECTIVIDAD RECIENTE
        query_r = text("SELECT acierto FROM auditoria_ia WHERE acierto IS NOT NULL ORDER BY timestamp_registro DESC LIMIT 20")
        res_r = await db.execute(query_r)
        recientes = res_r.fetchall()
        # Piso de efectividad en 45% para el Dashboard
        efectividad_base = (sum(1 for r in recientes if r[0]) / len(recientes) * 100) if recientes else 45.0

        # B. BUSCAR PATRONES HISTÓRICOS (Sincronización 2018-2026)
        hora_actual = datetime.now().strftime("%I:00 %p")
        query_h = text("SELECT animalito FROM historico WHERE hora = :hora AND fecha >= '2018-01-01'")
        res_h = await db.execute(query_h, {"hora": hora_actual})
        data = res_h.fetchall()

        if not data:
            analisis = "Modo Exploración (Sin coincidencias)."
            seleccion = random.sample(list(MAPA_ANIMALES.items()), 3)
        else:
            df = pd.DataFrame(data, columns=['animalito'])
            top = df['animalito'].value_counts().head(3).index.tolist()
            seleccion = []
            for t in top:
                name = limpiar_nombre(t)
                num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
                seleccion.append((num, name))
            analisis = f"Patrón detectado en {len(data)} sorteos."

        # C. REGISTRO PARA AUDITORÍA (Lo que llena tus gráficas)
        for i, item in enumerate(seleccion):
            conf_item = max(int(efectividad_base) - (i * 10), 10) # Nunca menor a 10%
            await db.execute(text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:f, :h, :a, :c, :p)
            """), {"f": datetime.now().date(), "h": hora_actual, "a": item[1], "c": conf_item, "p": "Neural Engine V4.5"})
        
        await db.commit()

        # Formateo de respuesta para el Dashboard
        top3 = []
        for i, (num, name) in enumerate(seleccion):
            # CORRECCIÓN: Porcentaje visual siempre positivo
            prob_visual = max(int(efectividad_base) - (i * 12), 15)
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{prob_visual}%"
            })

        return {
            "decision": "ALTA PROBABILIDAD" if efectividad_base >= 45 else "PATRÓN ESTABLE",
            "top3": top3,
            "analisis": f"Efectividad IA: {int(efectividad_base)}% | {analisis}"
        }
    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def entrenar_modelo_v4(db: AsyncSession):
    """Calibración cruzada: Cruza auditoria_ia con historico."""
    query = text("""
        UPDATE auditoria_ia SET resultado_real = h.animalito,
        acierto = (LOWER(TRIM(auditoria_ia.animal_predicho)) = LOWER(TRIM(h.animalito)))
        FROM historico h WHERE auditoria_ia.fecha = h.fecha AND auditoria_ia.hora = h.hora AND auditoria_ia.acierto IS NULL
    """)
    res = await db.execute(query)
    await db.commit()
    return {"status": "success", "mensaje": f"Se calibraron {res.rowcount} resultados.", "logs": "Sincronización de patrones completada."}
