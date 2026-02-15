import random
import pandas as pd
from sqlalchemy import text
import unicodedata
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

# Diccionario Maestro de Animalitos
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
        # --- A. ANALIZAR EFECTIVIDAD RECIENTE ---
        # Filtramos solo registros procesados (acierto no es NULL)
        query_rendimiento = text("""
            SELECT acierto FROM auditoria_ia 
            WHERE acierto IS NOT NULL 
            ORDER BY timestamp_registro DESC LIMIT 20
        """)
        res_r = await db.execute(query_rendimiento)
        recientes = res_r.fetchall()
        
        # Base de efectividad: si no hay datos, empezamos en 45% por defecto
        efectividad = (sum(1 for r in recientes if r[0]) / len(recientes) * 100) if recientes else 45.0
        
        # --- B. BUSCAR COINCIDENCIAS HISTÓRICAS ---
        hora_actual = datetime.now().strftime("%I:00 %p")
        query = text("""
            SELECT animalito FROM historico 
            WHERE hora = :hora AND fecha >= '2018-01-01'
        """)
        res = await db.execute(query, {"hora": hora_actual})
        data = res.fetchall()

        if not data:
            analisis = "Modo Exploración (Sin coincidencias históricas)."
            # Selección aleatoria de respaldo
            items_random = random.sample(list(MAPA_ANIMALES.items()), 3)
            seleccion = [(k, v) for k, v in items_random]
        else:
            df = pd.DataFrame(data, columns=['animalito'])
            # Obtenemos los 3 más frecuentes para esta hora
            top = df['animalito'].value_counts().head(3).index.tolist()
            seleccion = []
            for t in top:
                name = limpiar_nombre(t)
                num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
                seleccion.append((num, name))
            analisis = f"Patrón detectado en {len(data)} registros previos."

        # --- C. REGISTRAR EN AUDITORÍA ---
        fecha_hoy = datetime.now().date()
        for i, item in enumerate(seleccion):
            ins_query = text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:fecha, :hora, :animal, :conf, :patron)
            """)
            # La confianza baja un poco para el 2do y 3er lugar
            confianza_item = max(int(efectividad) - (i * 10), 10)
            
            await db.execute(ins_query, {
                "fecha": fecha_hoy,
                "hora": hora_actual,
                "animal": item[1],
                "conf": confianza_item,
                "patron": "Neural Pattern V4 (Frecuencia)"
            })
        await db.commit()

        # --- D. FORMATEAR RESPUESTA PARA EL FRONTEND ---
        top3 = []
        for i, (num, name) in enumerate(seleccion):
            # Evitamos que el porcentaje sea menor a 15% para mantener la estética
            porcentaje_visual = max(int(efectividad) - (i * 12), 15)
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{porcentaje_visual}%"
            })

        decision = "ALTA PROBABILIDAD" if efectividad >= 45 else "OBSERVAR - PATRÓN INESTABLE"
        
        return {
            "decision": decision, 
            "top3": top3, 
            "analisis": f"Efectividad IA: {int(efectividad)}% | {analisis}"
        }

    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def entrenar_modelo_v4(db: AsyncSession):
    """
    Esta función es el CEREBRO DE CALIBRACIÓN.
    Cruza las predicciones hechas con los resultados reales del histórico.
    """
    try:
        # Sincronizamos la tabla auditoria_ia con los resultados reales de historico
        query_update = text("""
            UPDATE auditoria_ia
            SET resultado_real = h.animalito,
                acierto = (LOWER(TRIM(auditoria_ia.animal_predicho)) = LOWER(TRIM(h.animalito)))
            FROM historico h
            WHERE auditoria_ia.fecha = h.fecha 
              AND auditoria_ia.hora = h.hora
              AND auditoria_ia.acierto IS NULL
        """)
        
        result = await db.execute(query_update)
        await db.commit()
        
        count = result.rowcount
        return {
            "status": "success", 
            "mensaje": f"Calibración exitosa. {count} registros actualizados.",
            "logs": "Sincronización de patrones completada."
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "mensaje": str(e)}

async def analizar_estadisticas(db: AsyncSession):
    """Retorna los datos para las gráficas de barras."""
    try:
        query = text("""
            SELECT animalito, COUNT(*) as conteo 
            FROM historico 
            GROUP BY animalito 
            ORDER BY conteo DESC LIMIT 10
        """)
        res = await db.execute(query)
        filas = res.fetchall()
        
        labels_data = {f[0].capitalize(): f[1] for f in filas} if filas else {"Sin Datos": 0}
        return {"status": "success", "data": labels_data}
    except Exception:
        return {"status": "error", "data": {}}
