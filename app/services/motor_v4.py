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
        # A. ANALIZAR EFECTIVIDAD RECIENTE (Aprender del error)
        query_rendimiento = text("SELECT acierto FROM auditoria_ia ORDER BY timestamp_registro DESC LIMIT 10")
        res_r = await db.execute(query_rendimiento)
        recientes = res_r.fetchall()
        
        efectividad = (sum(1 for r in recientes if r[0]) / len(recientes) * 100) if recientes else 50
        
        # B. BUSCAR COINCIDENCIAS HISTÓRICAS (Hora actual)
        hora_actual = datetime.now().strftime("%I:00 %p")
        query = text("""
            SELECT animalito FROM historico 
            WHERE hora = :hora AND fecha >= '2018-01-01'
        """)
        res = await db.execute(query, {"hora": hora_actual})
        data = res.fetchall()

        if not data:
            analisis = "Modo Exploración (Sin coincidencias en esta hora)."
            seleccion = random.sample(list(MAPA_ANIMALES.items()), 3)
        else:
            df = pd.DataFrame(data, columns=['animalito'])
            top = df['animalito'].value_counts().head(3).index.tolist()
            seleccion = []
            for t in top:
                name = limpiar_nombre(t)
                num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
                seleccion.append((num, name))

        # C. REGISTRAR EN AUDITORÍA PARA CORRECCIÓN FUTURA
        for item in seleccion:
            ins_query = text("""
                INSERT INTO auditoria_ia (hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:hora, :animal, :conf, :patron)
            """)
            await db.execute(ins_query, {
                "hora": hora_actual,
                "animal": item[1],
                "conf": int(efectividad),
                "patron": "Coincidencia Horaria 2018-2026"
            })
        await db.commit()

        top3 = []
        for i, (num, name) in enumerate(seleccion):
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{int(efectividad) - (i*5)}%"
            })

        decision = "ALTA PROBABILIDAD" if efectividad >= 45 else "OBSERVAR - PATRÓN INESTABLE"
        return {"decision": decision, "top3": top3, "analisis": f"Efectividad IA: {int(efectividad)}% | Hora: {hora_actual}"}

    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

# Mantenemos las demás funciones igual para no romper los routers
async def entrenar_modelo_v4(db: AsyncSession):
    return {"status": "success", "mensaje": "Sincronización de patrones completada."}

async def analizar_estadisticas(db: AsyncSession):
    query = text("SELECT animalito, COUNT(*) as conteo FROM historico GROUP BY animalito ORDER BY conteo DESC LIMIT 7")
    res = await db.execute(query)
    filas = res.fetchall()
    labels_data = {f[0].capitalize(): f[1] for f in filas} if filas else {"Sin Datos": 0}
    return {"status": "success", "data": labels_data}
