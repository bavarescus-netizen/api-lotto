"""
MOTOR V5 — LOTTOAI PRO
Cerebro de predicción con 5 señales combinadas.
Reemplaza: motor_v4.py, motor_prediccion_v2.py, backtest.py, calibrador.py
Tablas usadas: historico, probabilidades_hora, auditoria_ia, metricas
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
import pytz
import re

# ══════════════════════════════════════════════
# MAPA OFICIAL DE ANIMALES (0-36)
# ══════════════════════════════════════════════
MAPA_ANIMALES = {
    "0": "delfin", "00": "ballena", "1": "carnero", "2": "toro",
    "3": "ciempies", "4": "alacran", "5": "leon", "6": "rana",
    "7": "perico", "8": "raton", "9": "aguila", "10": "tigre",
    "11": "gato", "12": "caballo", "13": "mono", "14": "paloma",
    "15": "zorro", "16": "oso", "17": "pavo", "18": "burro",
    "19": "chivo", "20": "cochino", "21": "gallo", "22": "camello",
    "23": "cebra", "24": "iguana", "25": "gallina", "26": "vaca",
    "27": "perro", "28": "zamuro", "29": "elefante", "30": "caiman",
    "31": "lapa", "32": "ardilla", "33": "pescado", "34": "venado",
    "35": "jirafa", "36": "culebra"
}

NUMERO_POR_ANIMAL = {v: k for k, v in MAPA_ANIMALES.items()}

# ══════════════════════════════════════════════
# PESOS DE CADA SEÑAL (suman 1.0)
# ══════════════════════════════════════════════
PESO_FRECUENCIA_HORA   = 0.30  # Señal 1: Frecuencia histórica por hora
PESO_TENDENCIA_RECIENTE = 0.25  # Señal 2: Últimos 15 días en esa hora
PESO_CICLO_APARICION   = 0.20  # Señal 3: Sorteos desde última aparición
PESO_SECUENCIA         = 0.15  # Señal 4: Qué animal suele seguir al anterior
PESO_DIA_SEMANA        = 0.10  # Señal 5: Patrón por día de semana + hora


# ══════════════════════════════════════════════
# FUNCIÓN PRINCIPAL: GENERAR PREDICCIÓN
# ══════════════════════════════════════════════
async def generar_prediccion(db: AsyncSession) -> dict:
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p").upper()   # "10:00 AM"
        hora_int = int(ahora.strftime("%I")) if ahora.strftime("%p") == "AM" else int(ahora.strftime("%I")) + 12
        if hora_int == 12 and ahora.strftime("%p") == "AM":
            hora_int = 0
        if hora_int == 24:
            hora_int = 12
        dia_semana = ahora.weekday()  # 0=lunes ... 6=domingo

        scores = {}

        # ─────────────────────────────────────────
        # SEÑAL 1: Frecuencia histórica por hora
        # ¿Qué animal sale más en ESTA hora en toda la historia?
        # ─────────────────────────────────────────
        res1 = await db.execute(text("""
            SELECT animalito, COUNT(*) as total
            FROM historico
            WHERE hora = :hora
            GROUP BY animalito
            ORDER BY total DESC
        """), {"hora": hora_str})
        filas1 = res1.fetchall()

        if filas1:
            total_s1 = sum(r[1] for r in filas1)
            for r in filas1:
                scores[r[0]] = scores.get(r[0], 0) + (r[1] / total_s1) * PESO_FRECUENCIA_HORA

        # ─────────────────────────────────────────
        # SEÑAL 2: Tendencia reciente (últimos 15 días, misma hora)
        # ─────────────────────────────────────────
        fecha_15 = (ahora - timedelta(days=15)).strftime("%Y-%m-%d")
        res2 = await db.execute(text("""
            SELECT animalito, COUNT(*) as total
            FROM historico
            WHERE hora = :hora AND fecha >= :fecha_inicio
            GROUP BY animalito
            ORDER BY total DESC
        """), {"hora": hora_str, "fecha_inicio": fecha_15})
        filas2 = res2.fetchall()

        if filas2:
            total_s2 = sum(r[1] for r in filas2)
            for r in filas2:
                scores[r[0]] = scores.get(r[0], 0) + (r[1] / total_s2) * PESO_TENDENCIA_RECIENTE

        # ─────────────────────────────────────────
        # SEÑAL 3: Ciclo de aparición
        # Si un animal tarda normalmente X sorteos en aparecer
        # y ya pasaron más de X sorteos → aumenta su probabilidad
        # ─────────────────────────────────────────
        res3 = await db.execute(text("""
            WITH apariciones AS (
                SELECT 
                    animalito,
                    fecha,
                    hora,
                    ROW_NUMBER() OVER (ORDER BY fecha DESC, hora DESC) as rn
                FROM historico
            ),
            ciclos AS (
                SELECT 
                    animalito,
                    AVG(rn) as posicion_promedio,
                    MIN(rn) as ultima_posicion
                FROM apariciones
                GROUP BY animalito
            )
            SELECT animalito, ultima_posicion, posicion_promedio
            FROM ciclos
            ORDER BY ultima_posicion DESC
        """))
        filas3 = res3.fetchall()

        if filas3:
            max_pos = max(r[1] for r in filas3) or 1
            for r in filas3:
                # Mientras más sorteos han pasado sin aparecer, mayor score
                score_ciclo = (r[1] / max_pos)
                scores[r[0]] = scores.get(r[0], 0) + score_ciclo * PESO_CICLO_APARICION

        # ─────────────────────────────────────────
        # SEÑAL 4: Secuencia — qué animal suele salir DESPUÉS del último resultado
        # ─────────────────────────────────────────
        res_ultimo = await db.execute(text("""
            SELECT animalito FROM historico
            ORDER BY fecha DESC, hora DESC
            LIMIT 1
        """))
        ultimo = res_ultimo.scalar()

        if ultimo:
            res4 = await db.execute(text("""
                WITH secuencia AS (
                    SELECT 
                        animalito as actual,
                        LEAD(animalito) OVER (ORDER BY fecha, hora) as siguiente
                    FROM historico
                )
                SELECT siguiente, COUNT(*) as veces
                FROM secuencia
                WHERE actual = :ultimo AND siguiente IS NOT NULL
                GROUP BY siguiente
                ORDER BY veces DESC
                LIMIT 10
            """), {"ultimo": ultimo})
            filas4 = res4.fetchall()

            if filas4:
                total_s4 = sum(r[1] for r in filas4)
                for r in filas4:
                    scores[r[0]] = scores.get(r[0], 0) + (r[1] / total_s4) * PESO_SECUENCIA

        # ─────────────────────────────────────────
        # SEÑAL 5: Patrón día de semana + hora
        # ─────────────────────────────────────────
        res5 = await db.execute(text("""
            SELECT animalito, COUNT(*) as total
            FROM historico
            WHERE hora = :hora
              AND EXTRACT(DOW FROM fecha) = :dia
            GROUP BY animalito
            ORDER BY total DESC
        """), {"hora": hora_str, "dia": dia_semana})
        filas5 = res5.fetchall()

        if filas5:
            total_s5 = sum(r[1] for r in filas5)
            for r in filas5:
                scores[r[0]] = scores.get(r[0], 0) + (r[1] / total_s5) * PESO_DIA_SEMANA

        # ─────────────────────────────────────────
        # RANKING FINAL: ordenar por score total
        # ─────────────────────────────────────────
        if not scores:
            return {"top3": [], "analisis": "Sin datos suficientes para esta hora"}

        total_scores = sum(scores.values())
        ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        top3 = []
        for animal, score in ranking[:3]:
            nombre_limpio = re.sub(r'[^a-z]', '', animal.lower())
            num = NUMERO_POR_ANIMAL.get(nombre_limpio, "--")
            pct = round((score / total_scores) * 100, 1)
            top3.append({
                "numero": num,
                "animal": nombre_limpio.upper(),
                "imagen": f"{nombre_limpio}.png",
                "porcentaje": f"{pct}%",
                "score_raw": round(score, 4)
            })

        # ─────────────────────────────────────────
        # GUARDAR PREDICCIÓN EN auditoria_ia
        # ─────────────────────────────────────────
        if top3:
            try:
                await db.execute(text("""
                    INSERT INTO auditoria_ia (fecha, hora, animal_predicho, confianza_pct, resultado_real)
                    VALUES (:f, :h, :a, :c, 'PENDIENTE')
                    ON CONFLICT (fecha, hora) DO NOTHING
                """), {
                    "f": ahora.date(),
                    "h": hora_str,
                    "a": top3[0]["animal"].lower(),
                    "c": float(top3[0]["porcentaje"].replace('%', ''))
                })
                await db.commit()
            except Exception:
                await db.rollback()

        total_registros = sum(r[1] for r in filas1) if filas1 else 0

        return {
            "top3": top3,
            "hora": hora_str,
            "ultimo_resultado": ultimo or "N/A",
            "analisis": f"Motor V5 | {hora_str} | {total_registros} registros en esta hora | 5 señales activas"
        }

    except Exception as e:
        print(f"❌ Error en Motor V5: {e}")
        return {"top3": [], "analisis": f"Error: {str(e)}"}


# ══════════════════════════════════════════════
# ENTRENAR: Recalcular probabilidades_hora
# ══════════════════════════════════════════════
async def entrenar_modelo(db: AsyncSession) -> dict:
    try:
        # Limpiar tabla
        await db.execute(text("DELETE FROM probabilidades_hora"))

        # Insertar frecuencias + tendencia por hora
        await db.execute(text("""
            INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia, ultima_actualizacion)
            WITH base AS (
                SELECT 
                    CASE 
                        WHEN hora LIKE '%PM' AND hora NOT LIKE '12%' 
                            THEN CAST(SPLIT_PART(hora, ':', 1) AS INT) + 12
                        WHEN hora LIKE '12%AM' THEN 0
                        WHEN hora LIKE '12%PM' THEN 12
                        ELSE CAST(SPLIT_PART(hora, ':', 1) AS INT)
                    END as hora_int,
                    animalito,
                    COUNT(*) as total_hist
                FROM historico
                GROUP BY 1, 2
            ),
            reciente AS (
                SELECT 
                    CASE 
                        WHEN hora LIKE '%PM' AND hora NOT LIKE '12%' 
                            THEN CAST(SPLIT_PART(hora, ':', 1) AS INT) + 12
                        WHEN hora LIKE '12%AM' THEN 0
                        WHEN hora LIKE '12%PM' THEN 12
                        ELSE CAST(SPLIT_PART(hora, ':', 1) AS INT)
                    END as hora_int,
                    animalito,
                    COUNT(*) as total_rec
                FROM historico
                WHERE fecha >= CURRENT_DATE - INTERVAL '15 days'
                GROUP BY 1, 2
            ),
            totales AS (
                SELECT hora_int, SUM(total_hist) as gran_total
                FROM base GROUP BY hora_int
            )
            SELECT 
                b.hora_int,
                b.animalito,
                b.total_hist,
                (b.total_hist::FLOAT / NULLIF(t.gran_total, 0)) * 100 as prob,
                CASE WHEN COALESCE(r.total_rec, 0) >= 2 THEN 'CALIENTE' ELSE 'FRIO' END,
                NOW()
            FROM base b
            JOIN totales t ON b.hora_int = t.hora_int
            LEFT JOIN reciente r ON b.hora_int = r.hora_int AND b.animalito = r.animalito
            WHERE b.hora_int BETWEEN 7 AND 19
        """))

        # Contar registros totales en historico
        res_count = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total = res_count.scalar() or 0

        # Actualizar métricas en tabla 'metricas' (la correcta)
        await db.execute(text("""
            UPDATE metricas SET
                total = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL),
                aciertos = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = TRUE),
                errores = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = FALSE),
                precision = (
                    SELECT CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE (COUNT(CASE WHEN acierto = TRUE THEN 1 END)::FLOAT / COUNT(*)) * 100
                    END FROM auditoria_ia WHERE acierto IS NOT NULL
                ),
                fecha = NOW()
            WHERE id = 1
        """))

        await db.commit()

        return {
            "status": "success",
            "message": f"✅ Motor V5 entrenado. {total:,} registros analizados. Probabilidades actualizadas.",
            "registros_analizados": total
        }

    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": f"Error: {str(e)}"}


# ══════════════════════════════════════════════
# CALIBRAR: Validar predicciones pendientes
# ══════════════════════════════════════════════
async def calibrar_predicciones(db: AsyncSession) -> dict:
    try:
        result = await db.execute(text("""
            UPDATE auditoria_ia a
            SET 
                acierto = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha
              AND a.hora = h.hora
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE')
        """))
        calibradas = result.rowcount
        await db.commit()
        return {"calibradas": calibradas}
    except Exception as e:
        await db.rollback()
        return {"calibradas": 0, "error": str(e)}


# ══════════════════════════════════════════════
# OBTENER BITÁCORA DEL DÍA
# ══════════════════════════════════════════════
async def obtener_bitacora(db: AsyncSession) -> list:
    try:
        res = await db.execute(text("""
            SELECT 
                a.hora,
                a.animal_predicho,
                COALESCE(a.resultado_real, 'PENDIENTE') as resultado_real,
                a.acierto,
                a.confianza_pct
            FROM auditoria_ia a
            WHERE a.fecha = CURRENT_DATE
            ORDER BY a.hora DESC
            LIMIT 11
        """))
        bitacora = []
        for r in res.fetchall():
            nombre_real = re.sub(r'[^a-z]', '', (r[2] or '').lower())
            nombre_pred = re.sub(r'[^a-z]', '', (r[1] or '').lower())
            bitacora.append({
                "hora": r[0],
                "animal_predicho": nombre_pred.upper() if nombre_pred else "PENDIENTE",
                "resultado_real": nombre_real.upper() if nombre_real else "PENDIENTE",
                "acierto": r[3],
                "img_predicho": f"{nombre_pred}.png" if nombre_pred else "pendiente.png",
                "img_real": f"{nombre_real}.png" if nombre_real and nombre_real != 'pendiente' else "pendiente.png",
                "confianza": f"{round(r[4], 1)}%" if r[4] else "N/A"
            })
        return bitacora
    except Exception as e:
        print(f"❌ Error en bitácora: {e}")
        return []


# ══════════════════════════════════════════════
# OBTENER ESTADÍSTICAS GENERALES
# ══════════════════════════════════════════════
async def obtener_estadisticas(db: AsyncSession) -> dict:
    try:
        # Efectividad global
        res_ef = await db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN acierto = TRUE THEN 1 END) as aciertos,
                ROUND(
                    (COUNT(CASE WHEN acierto = TRUE THEN 1 END)::FLOAT / 
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END), 0)) * 100
                , 1) as precision
            FROM auditoria_ia
        """))
        ef = res_ef.fetchone()

        # Aciertos hoy
        res_hoy = await db.execute(text("""
            SELECT 
                COUNT(CASE WHEN acierto = TRUE THEN 1 END) as aciertos_hoy,
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END) as sorteos_completados
            FROM auditoria_ia
            WHERE fecha = CURRENT_DATE
        """))
        hoy = res_hoy.fetchone()

        # Top 5 animales más frecuentes (todos los horarios)
        res_top = await db.execute(text("""
            SELECT animalito, COUNT(*) as veces
            FROM historico
            GROUP BY animalito
            ORDER BY veces DESC
            LIMIT 5
        """))
        top_animales = [{"animal": r[0], "veces": r[1]} for r in res_top.fetchall()]

        # Total registros
        res_total = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_total.scalar() or 0

        return {
            "efectividad_global": float(ef[2] or 0),
            "total_auditado": int(ef[0] or 0),
            "aciertos_total": int(ef[1] or 0),
            "aciertos_hoy": int(hoy[0] or 0),
            "sorteos_hoy": int(hoy[1] or 0),
            "top_animales": top_animales,
            "total_historico": total_hist
        }
    except Exception as e:
        print(f"❌ Error estadísticas: {e}")
        return {"efectividad_global": 0, "aciertos_hoy": 0, "sorteos_hoy": 0}
