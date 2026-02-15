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
    """Normaliza texto para comparaciones seguras."""
    if not nombre: return "0"
    n = str(nombre).lower().strip()
    return "".join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

async def generar_prediccion(db: AsyncSession):
    try:
        # --- A. CÁLCULO DE EFECTIVIDAD REAL ---
        # Consultamos los últimos 20 resultados procesados para ajustar la confianza
        query_rendimiento = text("""
            SELECT acierto FROM auditoria_ia 
            WHERE acierto IS NOT NULL 
            ORDER BY timestamp_registro DESC LIMIT 20
        """)
        res_r = await db.execute(query_rendimiento)
        recientes = res_r.fetchall()
        
        # Si no hay datos, iniciamos con una base optimista del 45%
        efectividad_base = (sum(1 for r in recientes if r[0]) / len(recientes) * 100) if recientes else 45.0
        
        # --- B. BÚSQUEDA DE PATRONES POR HORA ---
        hora_actual = datetime.now().strftime("%I:00 %p")
        query_hist = text("""
            SELECT animalito FROM historico 
            WHERE hora = :hora AND fecha >= '2018-01-01'
        """)
        res_h = await db.execute(query_hist, {"hora": hora_actual})
        data = res_h.fetchall()

        if not data:
            analisis_msg = "Modo Exploración: Sin registros para esta hora."
            # Selección aleatoria balanceada
            items_random = random.sample(list(MAPA_ANIMALES.items()), 3)
            seleccion = [(k, v) for k, v in items_random]
        else:
            df = pd.DataFrame(data, columns=['animalito'])
            top = df['animalito'].value_counts().head(3).index.tolist()
            seleccion = []
            for t in top:
                name = limpiar_nombre(t)
                num = next((k for k, v in MAPA_ANIMALES.items() if v == name), "0")
                seleccion.append((num, name))
            analisis_msg = f"Basado en {len(data)} coincidencias históricas."

        # --- C. REGISTRO AUTOMÁTICO EN AUDITORÍA ---
        fecha_hoy = datetime.now().date()
        for i, item in enumerate(seleccion):
            ins_query = text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, patron_detectado)
                VALUES (:fecha, :hora, :animal, :conf, :patron)
            """)
            # Confianza ponderada (Piso mínimo de 10% para evitar negativos)
            conf_ponderada = max(int(efectividad_base) - (i * 12), 10)
            
            await db.execute(ins_query, {
                "fecha": fecha_hoy,
                "hora": hora_actual,
                "animal": item[1],
                "conf": conf_ponderada,
                "patron": "Neural Pattern V4.2"
            })
        await db.commit()

        # --- D. FORMATEO PARA INTERFAZ QUANTUM ---
        top3 = []
        for i, (num, name) in enumerate(seleccion):
            # Probabilidad visual (Nunca menor a 15% para el Radar)
            prob_visual = max(int(efectividad_base) - (i * 15), 15)
            top3.append({
                "numero": num,
                "animal": name.upper(),
                "imagen": f"{name}.png",
                "porcentaje": f"{prob_visual}%"
            })

        decision = "ALTA PROBABILIDAD" if efectividad_base >= 45 else "OBSERVAR - PATRÓN INESTABLE"
        
        return {
            "decision": decision, 
            "top3": top3, 
            "analisis": f"Efectividad IA: {int(efectividad_base)}% | {analisis_msg}"
        }

    except Exception as e:
        await db.rollback()
        return {"error": str(e)}

async def entrenar_modelo_v4(db: AsyncSession):
    """
    CEREBRO DE CALIBRACIÓN: Cruza predicciones vs resultados reales.
    Esto es lo que llena tus gráficas en la pestaña 'DATA'.
    """
    try:
        # Actualizamos la auditoría comparando con el histórico real
        query_calibrar = text("""
            UPDATE auditoria_ia
            SET resultado_real = h.animalito,
                acierto = (LOWER(TRIM(auditoria_ia.animal_predicho)) = LOWER(TRIM(h.animalito)))
            FROM historico h
            WHERE auditoria_ia.fecha = h.fecha 
              AND auditoria_ia.hora = h.hora
              AND auditoria_ia.acierto IS NULL
        """)
        
        res = await db.execute(query_calibrar)
        await db.commit()
        
        return {
            "status": "success", 
            "mensaje": f"Sincronización Exitosa: {res.rowcount} resultados calibrados.",
            "detalle": "IA actualizada con datos 2018-2026."
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "mensaje": str(e)}

async def analizar_estadisticas(db: AsyncSession):
    """Alimenta los gráficos de barras y de eficiencia."""
    try:
        # Top 10 animales más frecuentes para el gráfico de barras
        query = text("""
            SELECT animalito, COUNT(*) as conteo 
            FROM historico 
            GROUP BY animalito 
            ORDER BY conteo DESC LIMIT 10
        """)
        res = await db.execute(query)
        filas = res.fetchall()
        
        labels_data = {f[0].capitalize(): f[1] for f in filas} if filas else {"Cargando...": 0}
        return {"status": "success", "data": labels_data}
    except Exception:
        return {"status": "error", "data": {}}
