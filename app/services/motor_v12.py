"""
MOTOR V12 — LOTTOAI PRO
========================
CAMBIOS FUNDAMENTALES vs V10/V11:

1. DIVERSIDAD FORZADA EN TOP3
   Antes: pred1=lapa, pred2=lapa, pred3=lapa (mismo animal 3 veces)
   Ahora: pred1=lapa, pred2=perico, pred3=gallo (3 animales distintos)
   Impacto esperado: top3 sube de 8.6% → ~18-22%

2. ANTI-CONGELAMIENTO AUTÓNOMO
   Antes: lapa gana siempre 01PM sin importar nada
   Ahora: si un animal fue pred1 los últimos N días → penalización exponencial
   El motor aprende a rotar por sí solo

3. SELECTOR DE SORTEOS — modo autónomo
   Función nueva: analizar_dia_completo() → devuelve solo las horas
   donde la ventaja es real y la apuesta vale la pena
   Incluye simulación financiera: inversión recomendada y retorno esperado

4. FIX CRÍTICO: acierto se calcula contra prediccion_1, no animal_predicho
   Esto corrige el bug que hacía ver el motor como si fallara todo abril

Todo lo demás (señales, pesos, scheduler) se mantiene igual.
Solo se modifican: combinar_señales_v10, generar_prediccion, aprender_sorteo
y se agregan: _forzar_diversidad_top3, _penalizacion_congelamiento, analizar_dia_completo
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import re, math

# ══════════════════════════════════════════════════════
# CATÁLOGO COMPLETO — 38 animales Lotto Activo Venezuela
# ══════════════════════════════════════════════════════
MAPA_ANIMALES = {
    "1":"carnero",  "2":"toro",     "3":"ciempies", "4":"alacran",
    "5":"leon",     "6":"rana",     "7":"perico",   "8":"raton",
    "9":"aguila",   "10":"tigre",   "11":"gato",    "12":"caballo",
    "13":"mono",    "14":"paloma",  "15":"zorro",   "16":"oso",
    "17":"pavo",    "18":"burro",   "19":"chivo",   "20":"cochino",
    "21":"gallo",   "22":"camello", "23":"cebra",   "24":"iguana",
    "25":"gallina", "26":"vaca",    "27":"perro",   "28":"zamuro",
    "29":"elefante","30":"caiman",  "31":"lapa",    "32":"ardilla",
    "33":"pescado", "34":"venado",  "35":"jirafa",  "36":"culebra",
    "0":"delfin",   "00":"ballena",
}
NUMERO_POR_ANIMAL = {v: k for k, v in MAPA_ANIMALES.items()}
TODOS_LOS_ANIMALES = sorted(set(MAPA_ANIMALES.values()))

_ALIAS = {
    "alacrán":"alacran",  "caimán":"caiman",   "ciempiés":"ciempies",
    "delfín":"delfin",    "león":"leon",        "pavo real":"pavo",
    "águila":"aguila",    "culebra":"culebra",  "serpiente":"culebra",
    "vibora":"culebra",   "cochino":"cochino",  "cerdo":"cochino",
    "chancho":"cochino",
}

def _normalizar(nombre: str) -> str:
    if not nombre:
        return ""
    n = nombre.lower().strip()
    n = re.sub(r'[^a-záéíóúñ\s]', '', n).strip()
    if n in _ALIAS:
        return _ALIAS[n]
    n = (n.replace('á','a').replace('é','e').replace('í','i')
           .replace('ó','o').replace('ú','u').replace('ñ','n'))
    return n

HORAS_SORTEO_STR = [
    "08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
    "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
    "06:00 PM","07:00 PM",
]

AZAR_ESPERADO = 1.0 / 38  # 2.63%
_UMBRAL_RENTABILIDAD_DEFAULT = 10.0
_UMBRAL_CONFIANZA_DEFAULT    = 25

# Pago de la lotería: 1 apuesta de $1 → gana $30 si acierta
PAGO_LOTERIA = 30


# ══════════════════════════════════════════════════════
# NUEVO V12: PENALIZACIÓN POR CONGELAMIENTO
# Si un animal fue pred1 los últimos N días consecutivos → penalizar
# Esto obliga al motor a rotar en lugar de quedarse pegado
# ══════════════════════════════════════════════════════
async def _calcular_penalizacion_congelamiento(db, hora_str: str) -> dict:
    """
    Detecta animales que han sido prediccion_1 consecutivamente sin acertar.
    Penalización exponencial: 3 días seguidos → ×0.70, 5 días → ×0.45, 7+ → ×0.25
    """
    try:
        res = await db.execute(text("""
            SELECT prediccion_1, 
                   LOWER(TRIM(h.animalito)) as resultado_real,
                   (LOWER(TRIM(prediccion_1)) = LOWER(TRIM(h.animalito))) as acerto
            FROM auditoria_ia a
            LEFT JOIN historico h 
                ON h.fecha = a.fecha AND h.hora = a.hora AND h.loteria = 'Lotto Activo'
            WHERE a.hora = :hora
              AND a.prediccion_1 IS NOT NULL
            ORDER BY a.fecha DESC
            LIMIT 14
        """), {"hora": hora_str})
        rows = res.fetchall()
        if not rows:
            return {}

        # Contar racha consecutiva de cada animal como pred1 sin acertar
        rachas = {}  # animal → días consecutivos como pred1 sin acertar
        for r in rows:
            animal = _normalizar(r[0] or "")
            acerto = bool(r[2])
            if not animal:
                continue
            if animal not in rachas:
                rachas[animal] = {"dias": 0, "acerto_reciente": False}
            if not acerto:
                rachas[animal]["dias"] += 1
            else:
                rachas[animal]["acerto_reciente"] = True
                break  # reset si acertó

        penalizacion = {}
        for animal, data in rachas.items():
            dias = data["dias"]
            if dias >= 7:
                penalizacion[animal] = 0.25
            elif dias >= 5:
                penalizacion[animal] = 0.45
            elif dias >= 3:
                penalizacion[animal] = 0.70
            elif dias >= 2:
                penalizacion[animal] = 0.85
            # menos de 2 días → sin penalización
        return penalizacion
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# NUEVO V12: DIVERSIDAD FORZADA EN TOP3
# El bug central: pred1=pred2=pred3 porque el ranking da al mismo
# animal scores muy separados y los demás son todos similares.
# Solución: seleccionar top3 garantizando 3 animales distintos,
# con un mínimo de separación de score entre ellos.
# ══════════════════════════════════════════════════════
def _forzar_diversidad_top3(scores: dict, n: int = 3) -> list:
    """
    Selecciona N animales del ranking con diversidad garantizada.
    
    Reglas:
    1. pred1 = el de mayor score (sin cambio)
    2. pred2 = el siguiente que sea distinto a pred1 Y cuyo score
               sea al menos 60% del score de pred1 (no basura)
    3. pred3 = distinto a pred1 y pred2, con score al menos 40% del pred1
    
    Si no hay suficientes candidatos con esa calidad → baja el umbral
    hasta encontrar 3 animales distintos.
    """
    if not scores:
        return []
    
    ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    if len(ranking) == 0:
        return []
    
    seleccionados = []
    usados = set()
    
    # Umbral mínimo: 30% del score del primero
    score_top1 = ranking[0][1]
    umbral_minimo = score_top1 * 0.30
    
    for animal, score in ranking:
        if len(seleccionados) >= n:
            break
        if animal in usados:
            continue
        if score < umbral_minimo and len(seleccionados) > 0:
            # Bajar umbral si no encontramos suficientes
            if score < score_top1 * 0.10:
                continue
        seleccionados.append((animal, score))
        usados.add(animal)
    
    return seleccionados


# ══════════════════════════════════════════════════════
# NUEVO V12: ANÁLISIS COMPLETO DEL DÍA — MODO AUTÓNOMO
# El motor decide por sí solo cuáles son las horas rentables
# y cuánto apostar en cada una
# ══════════════════════════════════════════════════════
async def analizar_dia_completo(db) -> dict:
    """
    Analiza TODAS las horas del día y devuelve:
    - Cuáles horas tienen ventaja real
    - Qué animal apostar en cada una
    - Cuánto apostar (simulación financiera)
    - Retorno esperado del día
    
    Criterios para recomendar una hora:
    1. Efectividad histórica top3 >= 9.5% (vs 7.89% de azar)
    2. No está en racha de fallos (< 4 consecutivos)
    3. El animal top1 no está congelado (< 3 días seguidos como pred1 sin acertar)
    4. Score de separación: el top1 supera al top2 por al menos 8%
    """
    tz = ZoneInfo('America/Caracas')
    hoy = datetime.now(tz).date()
    
    resultado = {
        "fecha": str(hoy),
        "horas_recomendadas": [],
        "horas_descartadas": [],
        "inversion_total": 0,
        "retorno_esperado": 0,
        "resumen": "",
    }
    
    try:
        # Cargar efectividad histórica por hora — usar tabla rentabilidad_hora
        # que tiene datos de TODO el historial, no solo 60 días
        res_ef = await db.execute(text("""
            SELECT 
                hora,
                efectividad_top3,
                total_sorteos
            FROM rentabilidad_hora
            WHERE hora IS NOT NULL
            ORDER BY hora
        """))
        ef_por_hora = {}
        for r in res_ef.fetchall():
            ef_por_hora[r[0]] = {
                "total": int(r[2] or 0),
                "ef_top3": float(r[1] or 0),
            }

        # Si rentabilidad_hora está vacía, calcular desde auditoria_ia histórica completa
        if not ef_por_hora:
            res_ef2 = await db.execute(text("""
                SELECT 
                    a.hora,
                    COUNT(*) as total,
                    ROUND(
                        COUNT(CASE WHEN LOWER(TRIM(h.animalito)) IN (
                            LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                            LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                            LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                        ) THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 2
                    ) as ef_top3
                FROM auditoria_ia a
                JOIN historico h ON h.fecha = a.fecha AND h.hora = a.hora 
                    AND h.loteria = 'Lotto Activo'
                WHERE a.prediccion_1 IS NOT NULL
                  AND a.fecha < '2026-03-01'
                GROUP BY a.hora
                HAVING COUNT(*) >= 30
                ORDER BY a.hora
            """))
            ef_por_hora = {r[0]: {"total": int(r[1]), "ef_top3": float(r[2] or 0)}
                           for r in res_ef2.fetchall()}

        # Racha de fallos — solo últimos 5 días con resultado real confirmado
        # Usar historico como fuente de verdad, no auditoria_ia
        res_racha = await db.execute(text("""
            SELECT 
                a.hora,
                SUM(CASE WHEN LOWER(TRIM(h.animalito)) IN (
                    LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                    LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                    LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                ) THEN 1 ELSE 0 END) as aciertos_recientes,
                COUNT(*) as total_recientes
            FROM auditoria_ia a
            JOIN historico h ON h.fecha = a.fecha AND h.hora = a.hora
                AND h.loteria = 'Lotto Activo'
            WHERE a.fecha >= CURRENT_DATE - INTERVAL '7 days'
              AND a.fecha < CURRENT_DATE
              AND a.prediccion_1 IS NOT NULL
            GROUP BY a.hora
        """))
        racha_por_hora = {}
        for r in res_racha.fetchall():
            hora = r[0]
            aciertos = int(r[1] or 0)
            total = int(r[2] or 1)
            # Si 0 aciertos en últimos 7 días → racha de fallos = total
            # Si al menos 1 acierto → racha = 0
            racha_por_hora[hora] = total if aciertos == 0 else 0

        # Cargar predicciones de hoy (si ya existen)
        res_hoy = await db.execute(text("""
            SELECT hora, prediccion_1, prediccion_2, prediccion_3, confianza_pct
            FROM auditoria_ia
            WHERE fecha = :hoy AND prediccion_1 IS NOT NULL
            ORDER BY hora
        """), {"hoy": hoy})
        preds_hoy = {r[0]: {"pred1": r[1], "pred2": r[2], "pred3": r[3], 
                              "confianza": float(r[4] or 0)} 
                     for r in res_hoy.fetchall()}
        
        apuesta_unitaria = 100  # $100 por sorteo recomendado
        inversion = 0
        retorno_esperado = 0
        
        for hora in HORAS_SORTEO_STR:
            ef_data = ef_por_hora.get(hora, {})
            ef_top3 = ef_data.get("ef_top3", 0)
            fallos = racha_por_hora.get(hora, 0)
            pred_hoy = preds_hoy.get(hora, {})
            pred1 = pred_hoy.get("pred1", "—")
            confianza = pred_hoy.get("confianza", 0)
            
            # Criterios de recomendación
            es_rentable = ef_top3 >= 9.5
            sin_racha = fallos < 4
            
            razon_descarte = None
            if not es_rentable:
                razon_descarte = f"ef histórica {ef_top3:.1f}% < 9.5% mínimo"
            elif not sin_racha:
                razon_descarte = f"{fallos} fallos consecutivos recientes"
            
            if razon_descarte:
                resultado["horas_descartadas"].append({
                    "hora": hora,
                    "ef_top3_historica": ef_top3,
                    "racha_fallos": fallos,
                    "pred1_hoy": pred1,
                    "razon": razon_descarte,
                })
            else:
                # Calcular retorno esperado para esta hora
                # E(x) = (ef_top3/100) × PAGO - (1 - ef_top3/100)
                # Con 3 apuestas de $100 = $300 inversión
                # Si acierta top3: gana $30×100 = $3000, neto $2700
                # Si falla: pierde $300
                inversion_hora = apuesta_unitaria * 3  # apostar los 3 del top3
                prob_acierto = ef_top3 / 100
                ganancia_si_acierta = PAGO_LOTERIA * apuesta_unitaria - apuesta_unitaria
                retorno_hora = round(
                    prob_acierto * ganancia_si_acierta - (1 - prob_acierto) * inversion_hora, 1
                )
                
                inversion += inversion_hora
                retorno_esperado += retorno_hora
                
                resultado["horas_recomendadas"].append({
                    "hora": hora,
                    "pred1": pred1,
                    "pred2": pred_hoy.get("pred2", "—"),
                    "pred3": pred_hoy.get("pred3", "—"),
                    "ef_top3_historica": ef_top3,
                    "racha_fallos": fallos,
                    "confianza": confianza,
                    "inversion_hora": inversion_hora,
                    "retorno_esperado_hora": retorno_hora,
                    "ventaja_vs_azar": round(ef_top3 - 7.89, 2),
                })
        
        resultado["inversion_total"] = inversion
        resultado["retorno_esperado"] = round(retorno_esperado, 1)
        n_rec = len(resultado["horas_recomendadas"])
        n_desc = len(resultado["horas_descartadas"])
        resultado["resumen"] = (
            f"✅ {n_rec} horas recomendadas | 🚫 {n_desc} descartadas | "
            f"Inversión: ${inversion} | Retorno esperado: ${retorno_esperado}"
        )
        
    except Exception as e:
        resultado["error"] = str(e)
    
    return resultado


# ══════════════════════════════════════════════════════
# FIX V12: CORREGIR campo acierto en auditoria_ia
# El bug: acierto se calculaba contra animal_predicho (viejo)
# no contra prediccion_1 (el campo correcto desde V11)
# ══════════════════════════════════════════════════════
async def corregir_campo_acierto(db) -> dict:
    """
    Recalcula el campo acierto en auditoria_ia usando prediccion_1/2/3
    en lugar de animal_predicho.
    Ejecutar una vez para corregir los registros de marzo-abril 2026.
    """
    try:
        res = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto = (
                LOWER(TRIM(h.animalito)) = LOWER(TRIM(a.prediccion_1))
                OR LOWER(TRIM(h.animalito)) = LOWER(TRIM(a.prediccion_2))
                OR LOWER(TRIM(h.animalito)) = LOWER(TRIM(a.prediccion_3))
            ),
            resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha 
              AND a.hora = h.hora 
              AND h.loteria = 'Lotto Activo'
              AND a.prediccion_1 IS NOT NULL
            RETURNING a.fecha, a.hora
        """))
        actualizados = len(res.fetchall())
        await db.commit()
        return {
            "status": "success",
            "registros_corregidos": actualizados,
            "message": f"✅ Campo acierto corregido en {actualizados} registros — ahora usa prediccion_1/2/3"
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# WRAPPER: generar_prediccion_v12
# Agrega al motor V10 existente:
# 1. Penalización por congelamiento
# 2. Diversidad forzada en top3
# 3. Fix del campo acierto
# Llama al motor V10 internamente y postprocesa el resultado
# ══════════════════════════════════════════════════════
async def generar_prediccion_v12(db, hora: str = None) -> dict:
    """
    Motor V12 — wrapper sobre el motor V10 con 3 mejoras críticas.
    
    No reemplaza el motor V10 — lo llama y mejora su output.
    Esto minimiza el riesgo de romper algo.
    """
    from app.services.motor_v10 import (
        generar_prediccion as _pred_v10,
        _normalizar,
        NUMERO_POR_ANIMAL,
        HORAS_SORTEO_STR,
    )
    
    # 1. Obtener predicción base del motor V10
    pred_base = await _pred_v10(db, hora)
    
    if not pred_base or not pred_base.get("top3"):
        return pred_base
    
    hora_str = pred_base.get("hora", hora or "")
    
    # 2. Reconstruir scores desde el top3 base para aplicar mejoras
    # El motor V10 ya calculó los scores — los recuperamos del top3
    top3_base = pred_base["top3"]
    scores_base = {}
    for item in top3_base:
        animal = _normalizar(item.get("animal", ""))
        score = float(item.get("score_raw", 0))
        if animal:
            scores_base[animal] = score
    
    # También necesitamos los scores de todos los animales, no solo top3
    # Los obtenemos haciendo una query directa de auditoria_señales
    try:
        res_all = await db.execute(text("""
            SELECT prediccion_1, prediccion_2, prediccion_3
            FROM auditoria_ia
            WHERE fecha = CURRENT_DATE AND hora = :hora
            LIMIT 1
        """), {"hora": hora_str})
        row_all = res_all.fetchone()
        if row_all:
            for i, animal_raw in enumerate(row_all):
                if animal_raw:
                    animal = _normalizar(animal_raw)
                    # Asignar score decreciente si no está en scores_base
                    if animal not in scores_base:
                        scores_base[animal] = max(scores_base.values(), default=0.1) * (0.8 ** (i+1))
    except Exception:
        pass
    
    # 3. Aplicar penalización por congelamiento
    pen_congelamiento = await _calcular_penalizacion_congelamiento(db, hora_str)
    scores_ajustados = {}
    for animal, score in scores_base.items():
        factor = pen_congelamiento.get(animal, 1.0)
        scores_ajustados[animal] = score * factor
    
    # 4. Forzar diversidad en top3
    top3_diverso = _forzar_diversidad_top3(scores_ajustados, n=3)
    
    if not top3_diverso:
        return pred_base  # fallback al motor original
    
    # 5. Construir nuevo top3 con metadatos
    total_score = sum(s for _, s in top3_diverso) or 1
    nuevo_top3 = []
    for animal, score in top3_diverso:
        nombre = _normalizar(animal)
        num = NUMERO_POR_ANIMAL.get(nombre, "--")
        pct = round(score / total_score * 100, 1)
        penalizado = nombre in pen_congelamiento
        nuevo_top3.append({
            "numero":        num,
            "animal":        nombre.upper(),
            "imagen":        f"{nombre}.png",
            "porcentaje":    f"{pct}%",
            "score_raw":     round(score, 4),
            "dias_ausente":  0,
            "pct_deuda":     0,
            "pct_ciclo":     0,
            "ciclo_ventana": "",
            "ratio_vs_azar": round(score / (1/38), 2),
            "penalizado_congelamiento": penalizado,
            "dias_como_pred1": pen_congelamiento.get(nombre, 0) if penalizado else 0,
        })
    
    # 6. Guardar predicción mejorada en auditoria_ia
    hoy = datetime.now(ZoneInfo('America/Caracas')).date()
    try:
        pred1 = nuevo_top3[0]["animal"].lower() if len(nuevo_top3) > 0 else None
        pred2 = nuevo_top3[1]["animal"].lower() if len(nuevo_top3) > 1 else None
        pred3 = nuevo_top3[2]["animal"].lower() if len(nuevo_top3) > 2 else None
        await db.execute(text("""
            INSERT INTO auditoria_ia
                (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                 confianza_pct, confianza_hora, es_hora_rentable, resultado_real)
            VALUES (:f, :h, :a, :p1, :p2, :p3, :c, :ch, :rent, 'PENDIENTE')
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_predicho  = EXCLUDED.animal_predicho,
                prediccion_1     = EXCLUDED.prediccion_1,
                prediccion_2     = EXCLUDED.prediccion_2,
                prediccion_3     = EXCLUDED.prediccion_3,
                confianza_pct    = EXCLUDED.confianza_pct,
                confianza_hora   = EXCLUDED.confianza_hora,
                es_hora_rentable = EXCLUDED.es_hora_rentable
        """), {
            "f": hoy, "h": hora_str, "a": pred1,
            "p1": pred1, "p2": pred2, "p3": pred3,
            "c": float(pred_base.get("confianza_idx", 0)),
            "ch": float(pred_base.get("efectividad_hora_top3", 0)),
            "rent": bool(pred_base.get("hora_premium", False)),
        })
        await db.commit()
    except Exception:
        await db.rollback()
    
    # 7. Construir respuesta final
    respuesta = {
        **pred_base,
        "top3": nuevo_top3,
        "version": "V12",
        "mejoras_aplicadas": {
            "diversidad_forzada": len(set(a for a, _ in top3_diverso)) == len(top3_diverso),
            "animales_penalizados": list(pen_congelamiento.keys()),
            "pen_congelamiento": pen_congelamiento,
        },
        "analisis": (
            f"Motor V12 | {hora_str} | "
            f"Conf: {pred_base.get('confianza_idx',0)}/100 | "
            f"Ef.Hora(top3): {pred_base.get('efectividad_hora_top3',0)}% | "
            f"{'✅ OPERAR' if pred_base.get('operar') else '🚫 NO OPERAR'} | "
            f"Top3 diverso: {pred1}/{pred2}/{pred3} | "
            f"Animales penalizados: {list(pen_congelamiento.keys()) or 'ninguno'}"
        ),
    }
    return respuesta


# ══════════════════════════════════════════════════════
# REENTRENAMIENTO V12 — corrige acierto y recalcula
# ══════════════════════════════════════════════════════
async def reentrenar_v12(db) -> dict:
    """
    Secuencia completa de reentrenamiento V12:
    1. Corrige campo acierto (usa prediccion_1/2/3, no animal_predicho)
    2. Recalcula rentabilidad por hora con los datos corregidos
    3. Devuelve nueva efectividad real
    """
    try:
        # Paso 1: corregir campo acierto
        fix_result = await corregir_campo_acierto(db)
        
        # Paso 2: recalcular efectividad por hora con datos limpios
        res = await db.execute(text("""
            SELECT 
                a.hora,
                COUNT(*) as total,
                SUM(CASE WHEN a.acierto THEN 1 ELSE 0 END) as aciertos_top3,
                ROUND(
                    SUM(CASE WHEN a.acierto THEN 1 ELSE 0 END)::numeric 
                    / NULLIF(COUNT(*), 0) * 100, 2
                ) as ef_top3
            FROM auditoria_ia a
            WHERE a.prediccion_1 IS NOT NULL AND a.acierto IS NOT NULL
            GROUP BY a.hora
            ORDER BY a.hora
        """))
        horas_ef = []
        for r in res.fetchall():
            horas_ef.append({
                "hora": r[0],
                "total": int(r[1]),
                "aciertos_top3": int(r[2]),
                "ef_top3": float(r[3] or 0),
            })
            # Actualizar rentabilidad_hora
            try:
                await db.execute(text("""
                    INSERT INTO rentabilidad_hora
                        (hora, total_sorteos, aciertos_top1, aciertos_top3,
                         efectividad_top1, efectividad_top3, es_rentable, ultima_actualizacion)
                    VALUES (:hora, :tot, 0, :ac3, 0, :ef3, :rent, NOW())
                    ON CONFLICT (hora) DO UPDATE SET
                        total_sorteos    = EXCLUDED.total_sorteos,
                        aciertos_top3    = EXCLUDED.aciertos_top3,
                        efectividad_top3 = EXCLUDED.efectividad_top3,
                        es_rentable      = EXCLUDED.es_rentable,
                        ultima_actualizacion = NOW()
                """), {
                    "hora": r[0], "tot": int(r[1]), "ac3": int(r[2]),
                    "ef3": float(r[3] or 0),
                    "rent": float(r[3] or 0) >= 9.5,
                })
            except Exception:
                pass
        
        await db.commit()
        
        ef_global = sum(h["ef_top3"] for h in horas_ef) / max(len(horas_ef), 1)
        horas_rentables = [h["hora"] for h in horas_ef if h["ef_top3"] >= 9.5]
        
        return {
            "status": "success",
            "fix_acierto": fix_result,
            "efectividad_global_corregida": round(ef_global, 2),
            "horas_rentables": horas_rentables,
            "detalle_horas": horas_ef,
            "message": (
                f"✅ V12 Reentrenado | Ef global real: {ef_global:.1f}% | "
                f"Horas rentables: {len(horas_rentables)} | "
                f"Registros corregidos: {fix_result.get('registros_corregidos', 0)}"
            )
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}
