"""
MOTOR V13 — LOTTOAI PRO — ADAPTATIVO INTRADIARIO
=================================================
CAMBIOS v13.1:

REGLA A (reescrita):
  ANTES: resultado acertó en pred2/pred3 → subía ESE animal a pred1 en TODAS las horas siguientes
         Problema: el animal ya "se gastó", repetirlo es malo. Además propagaba a todas las horas.
  AHORA: usa tabla markov_transiciones para buscar qué animal sigue más frecuentemente
         después del resultado actual en la hora inmediata siguiente.
         Solo afecta la hora inmediata — no propaga a todas.

REGLA B (mejorada):
  Igual que antes pero también se activa cuando acierto_pos != "ninguna"
  si hay patrón con pct_confirmado >= 40%.

REGLA C (eliminada):
  "Animal caliente hoy" — causaba que un animal dominara todas las predicciones del día.

REGLA D (mejorada):
  5+ fallos consecutivos → recalcular hora siguiente desde probabilidades_hora reales
  en lugar de simplemente rotar pred1/pred2/pred3.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import re, math

# ══════════════════════════════════════════════════════
# CONSTANTES
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

HORAS_SORTEO_STR = [
    "08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
    "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
    "06:00 PM","07:00 PM",
]

PAGO_LOTERIA = 30

_ALIAS = {
    "alacrán":"alacran","caimán":"caiman","ciempiés":"ciempies",
    "delfín":"delfin","león":"leon","pavo real":"pavo",
    "águila":"aguila","serpiente":"culebra","vibora":"culebra",
    "cerdo":"cochino","chancho":"cochino",
}

def _norm(nombre: str) -> str:
    if not nombre: return ""
    n = nombre.lower().strip()
    n = re.sub(r'[^a-záéíóúñ\s]', '', n).strip()
    if n in _ALIAS: return _ALIAS[n]
    return (n.replace('á','a').replace('é','e').replace('í','i')
             .replace('ó','o').replace('ú','u').replace('ñ','n'))


# ══════════════════════════════════════════════════════
# MIGRACIÓN — crear tabla plan_dia si no existe
# ══════════════════════════════════════════════════════
async def migrar_tabla_plan_dia(db) -> None:
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS plan_dia (
            id              SERIAL PRIMARY KEY,
            fecha           DATE NOT NULL DEFAULT CURRENT_DATE,
            hora            VARCHAR(15) NOT NULL,
            pred1_original  VARCHAR(40),
            pred2_original  VARCHAR(40),
            pred3_original  VARCHAR(40),
            pred1_ajustada  VARCHAR(40),
            pred2_ajustada  VARCHAR(40),
            pred3_ajustada  VARCHAR(40),
            resultado_real  VARCHAR(40),
            acierto_pos     VARCHAR(10),
            motivo_ajuste   TEXT,
            fue_ajustada    BOOLEAN DEFAULT false,
            confianza       FLOAT DEFAULT 0,
            ef_hora_ponderada FLOAT DEFAULT 0,
            creado_en       TIMESTAMP DEFAULT NOW(),
            UNIQUE(fecha, hora)
        )
    """))
    await db.commit()


# ══════════════════════════════════════════════════════
# CARGAR PATRONES INTRADAY CONFIRMADOS
# ══════════════════════════════════════════════════════
async def _cargar_patrones(db) -> list:
    try:
        res = await db.execute(text("""
            SELECT trigger_hora, trigger_animal,
                   resultado_hora, resultado_animal,
                   pct_confirmado, n_casos, ventaja_vs_azar
            FROM patrones_intraday_confirmados
            WHERE activo = true AND n_casos >= 4
            ORDER BY pct_confirmado DESC
        """))
        return [dict(r._mapping) for r in res.fetchall()]
    except Exception:
        return []


# ══════════════════════════════════════════════════════
# MARKOV: buscar mejor animal siguiente
# ══════════════════════════════════════════════════════
async def _markov_siguiente(db, animal_previo: str, hora_sig: str) -> str | None:
    """
    Busca en markov_transiciones qué animal sigue más frecuentemente
    después de animal_previo en la hora hora_sig.
    Retorna el animal o None si no hay datos suficientes.
    """
    try:
        res = await db.execute(text("""
            SELECT animal_sig, frecuencia, probabilidad
            FROM markov_transiciones
            WHERE animal_previo = :prev
              AND hora = :hora
              AND frecuencia >= 3
            ORDER BY frecuencia DESC
            LIMIT 1
        """), {"prev": animal_previo, "hora": hora_sig})
        row = res.fetchone()
        if row and row[0]:
            return _norm(row[0])
        return None
    except Exception:
        return None


# ══════════════════════════════════════════════════════
# MEJOR PREDICCIÓN DESDE probabilidades_hora
# Para recalcular cuando hay muchos fallos
# ══════════════════════════════════════════════════════
async def _mejor_animal_hora(db, hora: str, excluir: list = None) -> list:
    """
    Retorna los top 3 animales para una hora según probabilidades_hora recientes.
    Excluye animales que ya salieron hoy (excluir).
    """
    excluir = excluir or []
    try:
        res = await db.execute(text("""
            SELECT animalito, probabilidad
            FROM probabilidades_hora
            WHERE hora = :hora
            ORDER BY probabilidad DESC
            LIMIT 10
        """), {"hora": hora})
        animales = [(row[0], row[1]) for row in res.fetchall()
                    if _norm(row[0]) not in excluir]
        return [_norm(a[0]) for a in animales[:3]]
    except Exception:
        return []


# ══════════════════════════════════════════════════════
# CALCULAR EFECTIVIDAD PONDERADA POR HORA
# ══════════════════════════════════════════════════════
async def _ef_ponderada_hora(db, hora: str) -> float:
    try:
        res = await db.execute(text("""
            SELECT
                COUNT(CASE WHEN a.fecha >= CURRENT_DATE-30 THEN 1 END) as t30,
                COUNT(CASE WHEN a.fecha >= CURRENT_DATE-30
                    AND LOWER(TRIM(h.animalito)) IN (
                        LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                    ) THEN 1 END) as a30,
                COUNT(CASE WHEN a.fecha >= CURRENT_DATE-90 THEN 1 END) as t90,
                COUNT(CASE WHEN a.fecha >= CURRENT_DATE-90
                    AND LOWER(TRIM(h.animalito)) IN (
                        LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                    ) THEN 1 END) as a90,
                COUNT(*) as th,
                COUNT(CASE WHEN LOWER(TRIM(h.animalito)) IN (
                    LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                    LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                    LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                ) THEN 1 END) as ah
            FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
                AND h.loteria='Lotto Activo'
            WHERE a.hora=:hora AND a.prediccion_1 IS NOT NULL
        """), {"hora": hora})
        r = res.fetchone()
        if not r: return 0.0
        t30,a30,t90,a90,th,ah = (int(x or 0) for x in r)
        ef30 = a30/t30*100  if t30  >= 5  else None
        ef90 = a90/t90*100  if t90  >= 15 else None
        efh  = ah/th*100    if th   >= 30 else None
        parts = [(ef30,3.0),(ef90,2.0),(efh,1.0)]
        num = sum(e*p for e,p in parts if e is not None)
        den = sum(p   for e,p in parts if e is not None)
        return round(num/den, 2) if den > 0 else 0.0
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════
# GENERAR PLAN DEL DÍA
# ══════════════════════════════════════════════════════
async def generar_plan_dia(db, fecha_objetivo: date = None) -> dict:
    from app.services.motor_v12 import generar_prediccion_v12

    tz = ZoneInfo('America/Caracas')
    if fecha_objetivo is None:
        fecha_objetivo = (datetime.now(tz) + timedelta(days=1)).date()

    await migrar_tabla_plan_dia(db)

    plan = []
    horas_rentables = []

    for hora in HORAS_SORTEO_STR:
        try:
            pred = await generar_prediccion_v12(db, hora)
            top3 = pred.get("top3", [])
            p1 = _norm(top3[0]["animal"]) if len(top3) > 0 else None
            p2 = _norm(top3[1]["animal"]) if len(top3) > 1 else None
            p3 = _norm(top3[2]["animal"]) if len(top3) > 2 else None
            ef = await _ef_ponderada_hora(db, hora)
            confianza = float(pred.get("confianza_idx", 0))
            es_rentable = ef >= 9.5

            await db.execute(text("""
                INSERT INTO plan_dia
                    (fecha, hora, pred1_original, pred2_original, pred3_original,
                     pred1_ajustada, pred2_ajustada, pred3_ajustada,
                     fue_ajustada, confianza, ef_hora_ponderada)
                VALUES (:f, :h, :p1, :p2, :p3, :p1, :p2, :p3, false, :c, :ef)
                ON CONFLICT (fecha, hora) DO UPDATE SET
                    pred1_original    = EXCLUDED.pred1_original,
                    pred2_original    = EXCLUDED.pred2_original,
                    pred3_original    = EXCLUDED.pred3_original,
                    pred1_ajustada    = EXCLUDED.pred1_ajustada,
                    pred2_ajustada    = EXCLUDED.pred2_ajustada,
                    pred3_ajustada    = EXCLUDED.pred3_ajustada,
                    fue_ajustada      = false,
                    confianza         = EXCLUDED.confianza,
                    ef_hora_ponderada = EXCLUDED.ef_hora_ponderada
            """), {"f": fecha_objetivo, "h": hora,
                   "p1": p1, "p2": p2, "p3": p3,
                   "c": confianza, "ef": ef})

            plan.append({
                "hora": hora, "pred1": p1, "pred2": p2, "pred3": p3,
                "ef_ponderada": ef, "confianza": confianza,
                "rentable": es_rentable,
            })
            if es_rentable:
                horas_rentables.append(hora)

        except Exception as e:
            plan.append({"hora": hora, "error": str(e)})

    await db.commit()
    return {
        "status": "success",
        "fecha": str(fecha_objetivo),
        "horas_planificadas": len(plan),
        "horas_rentables": horas_rentables,
        "plan": plan,
        "message": f"✅ Plan generado para {fecha_objetivo} — {len(horas_rentables)} horas rentables"
    }


# ══════════════════════════════════════════════════════
# AJUSTE INTRADIARIO — núcleo del motor V13
# ══════════════════════════════════════════════════════
async def ajustar_tras_sorteo(db, hora_actual: str, resultado_real: str, fecha_override=None) -> dict:
    """
    Después de cada sorteo real aplica 3 reglas en orden de prioridad:

    REGLA A — Markov (solo hora inmediata siguiente):
      Busca en markov_transiciones qué animal sigue más frecuentemente
      después del resultado actual. No repite el mismo animal.
      Solo aplica a la hora +1, no propaga a todo el día.

    REGLA B — Patrones confirmados (todas las horas siguientes):
      Si existe un patrón intraday confirmado (n_casos>=4, pct>=40%)
      para el resultado actual, lo aplica en la hora correspondiente.

    REGLA D — Reset por fallos (solo hora inmediata):
      Si hay 5+ fallos consecutivos, recalcula la hora siguiente
      usando las probabilidades_hora reales, excluyendo animales
      que ya salieron hoy.

    REGLA A ya no:
      - Repite el animal ganador en todas las horas siguientes
      - Actúa solo cuando acertó en pred2/pred3 (ahora actúa siempre)
    """
    tz = ZoneInfo('America/Caracas')
    hoy = fecha_override if fecha_override else datetime.now(tz).date()
    if isinstance(hoy, str):
        hoy = date.fromisoformat(hoy)

    resultado = _norm(resultado_real)
    ajustes_aplicados = []

    try:
        # ── 1. Obtener predicción de la hora actual ──
        res_actual = await db.execute(text("""
            SELECT pred1_ajustada, pred2_ajustada, pred3_ajustada
            FROM plan_dia
            WHERE fecha = :f AND hora = :h
        """), {"f": hoy, "h": hora_actual})
        row = res_actual.fetchone()

        if not row:
            return {"status": "no_plan", "message": f"No hay plan para {hora_actual}"}

        p1 = _norm(row[0] or "")
        p2 = _norm(row[1] or "")
        p3 = _norm(row[2] or "")

        # Determinar posición del acierto
        if resultado == p1:
            acierto_pos = "pred1"
        elif resultado == p2:
            acierto_pos = "pred2"
        elif resultado == p3:
            acierto_pos = "pred3"
        else:
            acierto_pos = "ninguna"

        # ── 2. Registrar resultado ──
        await db.execute(text("""
            UPDATE plan_dia SET
                resultado_real = :r,
                acierto_pos    = :pos
            WHERE fecha = :f AND hora = :h
        """), {"r": resultado, "pos": acierto_pos, "f": hoy, "h": hora_actual})

        # ── 3. Contar fallos consecutivos hoy ──
        res_fallos = await db.execute(text("""
            SELECT COUNT(*) FROM plan_dia
            WHERE fecha = :f
              AND resultado_real IS NOT NULL
              AND acierto_pos = 'ninguna'
              AND hora <= :h
        """), {"f": hoy, "h": hora_actual})
        fallos_hoy = int((res_fallos.fetchone() or [0])[0])

        # ── 4. Horas siguientes ──
        idx_actual = HORAS_SORTEO_STR.index(hora_actual) if hora_actual in HORAS_SORTEO_STR else -1
        horas_siguientes = HORAS_SORTEO_STR[idx_actual + 1:] if idx_actual >= 0 else []

        if not horas_siguientes:
            await db.commit()
            return {
                "status": "success",
                "hora": hora_actual,
                "resultado": resultado,
                "acierto_pos": acierto_pos,
                "ajustes": [],
                "message": "Último sorteo del día — no hay horas siguientes"
            }

        # ── 5. Cargar patrones confirmados para el resultado actual ──
        patrones = await _cargar_patrones(db)
        patrones_activos = [
            p for p in patrones
            if _norm(p["trigger_animal"]) == resultado
            and p["trigger_hora"] == hora_actual
            and float(p["pct_confirmado"]) >= 40.0
        ]

        # ── 6. Animales que ya salieron hoy (para excluir en reset) ──
        res_hoy_data = await db.execute(text("""
            SELECT resultado_real FROM plan_dia
            WHERE fecha = :f AND resultado_real IS NOT NULL
        """), {"f": hoy})
        animales_hoy = [_norm(r[0]) for r in res_hoy_data.fetchall() if r[0]]

        hora_inmediata = horas_siguientes[0]

        # ── 7. REGLA A — Markov (solo hora inmediata) ──
        animal_markov = await _markov_siguiente(db, resultado, hora_inmediata)

        # ── 8. Aplicar ajustes por hora ──
        for hora_sig in horas_siguientes:

            res_sig = await db.execute(text("""
                SELECT pred1_ajustada, pred2_ajustada, pred3_ajustada
                FROM plan_dia
                WHERE fecha = :f AND hora = :h
            """), {"f": hoy, "h": hora_sig})
            row_sig = res_sig.fetchone()
            if not row_sig:
                continue

            s1 = _norm(row_sig[0] or "")
            s2 = _norm(row_sig[1] or "")
            s3 = _norm(row_sig[2] or "")

            nuevo_p1, nuevo_p2, nuevo_p3 = s1, s2, s3
            motivo = None

            # ── REGLA A: Markov — solo hora inmediata siguiente ──
            if hora_sig == hora_inmediata and animal_markov and animal_markov != s1:
                nuevo_p1 = animal_markov
                nuevo_p2 = s1
                nuevo_p3 = s2
                motivo = (f"Markov: {resultado}@{hora_actual}"
                          f" → {animal_markov}@{hora_sig}")

            # ── REGLA B: Patrones confirmados (cualquier hora siguiente) ──
            if motivo is None:
                for patron in patrones_activos:
                    if patron["resultado_hora"] == hora_sig:
                        animal_patron = _norm(patron["resultado_animal"])
                        pct = float(patron["pct_confirmado"])
                        if animal_patron != s1:
                            nuevo_p1 = animal_patron
                            nuevo_p2 = s1
                            nuevo_p3 = s2
                            motivo = (f"Patrón {pct:.0f}%: "
                                      f"{resultado}@{hora_actual}"
                                      f" → {animal_patron}@{hora_sig}")
                        break

            # ── REGLA D: Reset por fallos — solo hora inmediata ──
            if motivo is None and hora_sig == hora_inmediata and fallos_hoy >= 5:
                mejores = await _mejor_animal_hora(db, hora_sig, excluir=animales_hoy)
                if len(mejores) >= 2:
                    nuevo_p1 = mejores[0]
                    nuevo_p2 = mejores[1]
                    nuevo_p3 = mejores[2] if len(mejores) > 2 else s3
                    motivo = (f"Reset probabilidades — {fallos_hoy} fallos hoy, "
                              f"excluidos: {', '.join(set(animales_hoy))[:40]}")

            # ── Guardar si hubo cambio ──
            if motivo:
                await db.execute(text("""
                    UPDATE plan_dia SET
                        pred1_ajustada = :p1,
                        pred2_ajustada = :p2,
                        pred3_ajustada = :p3,
                        fue_ajustada   = true,
                        motivo_ajuste  = :m
                    WHERE fecha = :f AND hora = :h
                """), {"p1": nuevo_p1, "p2": nuevo_p2, "p3": nuevo_p3,
                       "m": motivo, "f": hoy, "h": hora_sig})

                ajustes_aplicados.append({
                    "hora_ajustada": hora_sig,
                    "pred_anterior": f"{s1}/{s2}/{s3}",
                    "pred_nueva":    f"{nuevo_p1}/{nuevo_p2}/{nuevo_p3}",
                    "motivo": motivo,
                })

        await db.commit()

        return {
            "status": "success",
            "hora": hora_actual,
            "resultado_real": resultado,
            "acierto_pos": acierto_pos,
            "fallos_hoy": fallos_hoy,
            "markov_aplicado": animal_markov,
            "ajustes_aplicados": len(ajustes_aplicados),
            "detalle_ajustes": ajustes_aplicados,
            "message": (
                f"{'✅' if acierto_pos != 'ninguna' else '❌'} "
                f"{hora_actual}: {resultado} ({acierto_pos}) | "
                f"Markov→{animal_markov or 'N/A'} | "
                f"Ajustes: {len(ajustes_aplicados)} | "
                f"Fallos: {fallos_hoy}"
            )
        }

    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# DASHBOARD DEL DÍA
# ══════════════════════════════════════════════════════
async def dashboard_dia(db, fecha: date = None) -> dict:
    tz = ZoneInfo('America/Caracas')
    if fecha is None:
        fecha = datetime.now(tz).date()

    try:
        res = await db.execute(text("""
            SELECT
                hora,
                pred1_original, pred2_original, pred3_original,
                pred1_ajustada, pred2_ajustada, pred3_ajustada,
                resultado_real, acierto_pos,
                fue_ajustada, motivo_ajuste,
                confianza, ef_hora_ponderada
            FROM plan_dia
            WHERE fecha = :f
        """), {"f": fecha})
        rows_raw = res.fetchall()

        orden_horas = {h: i for i, h in enumerate(HORAS_SORTEO_STR)}
        rows = sorted(rows_raw, key=lambda r: orden_horas.get(r[0], 99))

        if not rows:
            return {
                "status": "sin_plan",
                "fecha": str(fecha),
                "message": "No hay plan para esta fecha. Ejecuta /plan/dia primero."
            }

        horas = []
        aciertos = 0
        fallos = 0
        pendientes = 0
        ganancia = 0.0
        apuesta_unitaria = 100

        for r in rows:
            hora, p1o, p2o, p3o, p1a, p2a, p3a, resultado, pos, ajustada, motivo, conf, ef = r

            estado = "pendiente"
            if resultado:
                if pos and pos != "ninguna":
                    estado = f"✅ {pos}"
                    aciertos += 1
                    ganancia += PAGO_LOTERIA * apuesta_unitaria - apuesta_unitaria
                else:
                    estado = "❌ fallo"
                    fallos += 1
                    ganancia -= apuesta_unitaria * 3
            else:
                pendientes += 1

            horas.append({
                "hora": hora,
                "pred_original": f"{p1o}/{p2o}/{p3o}",
                "pred_activa":   f"{p1a}/{p2a}/{p3a}",
                "ajustada": bool(ajustada),
                "motivo_ajuste": motivo,
                "resultado_real": resultado or "—",
                "estado": estado,
                "ef_ponderada": round(float(ef or 0), 1),
                "confianza": round(float(conf or 0), 1),
            })

        total_jugados = aciertos + fallos
        ef_real_hoy = round(aciertos / total_jugados * 100, 1) if total_jugados > 0 else 0

        return {
            "status": "success",
            "fecha": str(fecha),
            "resumen": {
                "aciertos": aciertos,
                "fallos": fallos,
                "pendientes": pendientes,
                "ef_real_hoy": ef_real_hoy,
                "ganancia_acumulada": round(ganancia, 0),
                "inversion_hasta_ahora": total_jugados * apuesta_unitaria * 3,
            },
            "horas": horas,
            "proximas_horas": [h for h in horas if h["resultado_real"] == "—"],
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# REENTRENAR V13
# ══════════════════════════════════════════════════════
async def reentrenar_v13(db) -> dict:
    try:
        await migrar_tabla_plan_dia(db)
        return {
            "status": "success",
            "message": "✅ V13 listo — tabla plan_dia creada. Ejecuta /plan/dia para generar el plan de mañana."
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
