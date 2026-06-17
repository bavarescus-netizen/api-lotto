"""
main.py — LOTTOAI PRO V11
==========================
CORRECCIONES APLICADAS:
  [FIX-1] Import incorrecto app.core.motor_v10 → app.services.motor_v10
  [FIX-2] Upsert Markov: ON CONFLICT DO UPDATE (antes era DO NOTHING)
  [FIX-3] Pesos en /estado ahora leen motor_pesos_hora real (no hardcodeados)
  [FIX-4] /resultado: endpoint que dispara aprendizaje automático post-sorteo
  [FIX-5] Limpieza de duplicados antes de aplicar constraint UNIQUE
  [FIX-6] decay_lambda ya no está hardcodeado en el response
  [FIX-7] Import motor_aprendizaje eliminado (era circular) → motor_v10 directo
  [FIX-8] /recuperar-pendientes: cruza historico con auditoria_ia masivamente
"""

import os, re, asyncio, datetime, logging
logger = logging.getLogger(__name__)  # ← AÑADIR
from fastapi import FastAPI, Request, Depends, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db, AsyncSessionLocal

from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito, startup

from app.services.motor_v10 import (
    generar_prediccion, obtener_estadisticas, obtener_bitacora,
    entrenar_modelo, backtest, calibrar_predicciones,
    llenar_auditoria_retroactiva, aprender_desde_historico,
    migrar_schema, actualizar_resultados_señales, obtener_score_señales,
    obtener_contexto_diario,
    aprender_sorteo, aprender_ultimos_n, obtener_historial_aprendizaje,
)

from app.services.motor_v12 import (
    generar_prediccion_v12,
    analizar_dia_completo,
    reentrenar_v12,
    corregir_campo_acierto,
)

from app.services.motor_v13 import (
    generar_plan_dia,
    ajustar_tras_sorteo,
    dashboard_dia,
    reentrenar_v13,
)

# [FIX-7] motor_aprendizaje eliminado — era circular e innecesario.
# aprender_tras_sorteo → aprender_sorteo de motor_v10 (misma firma)
# recalcular_todos_los_pesos → implementado aquí directamente
aprender_tras_sorteo = aprender_sorteo


async def recalcular_todos_los_pesos(db):
    """
    Recalibra pesos hora por hora usando aprendizaje_sorteo histórico.
    Reemplaza motor_aprendizaje.recalcular_todos_los_pesos() que era circular.
    """
    horas = [
        "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM",
        "12:00 PM", "01:00 PM", "02:00 PM", "03:00 PM",
        "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM",
    ]
    ok = 0
    for hora in horas:
        try:
            rows = (await db.execute(text("""
                SELECT acerto_top1, acerto_top3
                FROM aprendizaje_sorteo
                WHERE hora = :hora
                ORDER BY creado DESC LIMIT 60
            """), {"hora": hora})).fetchall()
            if not rows:
                continue
            total = len(rows)
            ac3   = sum(1 for r in rows if r[1])
            ef3   = ac3 / total
            # Ajustar pesos según efectividad histórica de esta hora
            if ef3 >= 0.10:
                pd, pm, pg, pr = 0.35, 0.25, 0.25, 0.15
            elif ef3 >= 0.085:
                pd, pm, pg, pr = 0.30, 0.25, 0.25, 0.20
            else:
                pd, pm, pg, pr = 0.25, 0.30, 0.25, 0.20
            await db.execute(text("""
                INSERT INTO motor_pesos_hora
                    (hora, generacion, peso_decay, peso_markov, peso_gap,
                     peso_reciente, efectividad, total_evaluados, aciertos_top3)
                VALUES (:hora, 1, :pd, :pm, :pg, :pr, :ef3, :total, :ac3)
                ON CONFLICT (hora, generacion) DO UPDATE SET
                    peso_decay      = :pd,
                    peso_markov     = :pm,
                    peso_gap        = :pg,
                    peso_reciente   = :pr,
                    efectividad     = :ef3,
                    total_evaluados = :total,
                    aciertos_top3   = :ac3,
                    fecha           = NOW()
            """), {
                "hora": hora, "pd": pd, "pm": pm, "pg": pg, "pr": pr,
                "ef3": round(ef3 * 100, 2), "total": total, "ac3": ac3,
            })
            await db.commit()
            ok += 1
        except Exception as e_h:
            await db.rollback()
            logger.warning(f"⚠️ recalcular_pesos hora {hora}: {e_h}")
    return {"ok": ok, "total": len(horas)}


logger = logging.getLogger(__name__)

# ── Estado global de tareas largas ──
_tarea = {
    "nombre": None,
    "estado": "idle",
    "progreso": "",
    "resultado": None,
    "iniciado": None,
}

async def _run_aprender(fecha_inicio):
    _tarea.update({"nombre": "aprender", "estado": "running",
                   "progreso": "Iniciando...", "resultado": None,
                   "iniciado": str(datetime.datetime.now())})
    try:
        async with AsyncSessionLocal() as db:
            res = await aprender_desde_historico(db, fecha_inicio)
        _tarea.update({"estado": "done", "progreso": "Completado", "resultado": res})
    except Exception as e:
        _tarea.update({"estado": "error", "progreso": str(e)})

async def _run_retroactivo(fd, fh, dias):
    _tarea.update({"nombre": "retroactivo", "estado": "running",
                   "progreso": "Iniciando retroactivo...", "resultado": None,
                   "iniciado": str(datetime.datetime.now())})
    try:
        async with AsyncSessionLocal() as db:
            res = await llenar_auditoria_retroactiva(db, fd, fh, dias)
        _tarea.update({"estado": "done", "progreso": "Completado", "resultado": res})
    except Exception as e:
        _tarea.update({"estado": "error", "progreso": str(e)})


app = FastAPI(title="LOTTOAI PRO V11 — Auto-Learning")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=False, allow_methods=["GET", "POST"], allow_headers=["*"])

app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(prediccion.router)
app.include_router(cargarhist.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))


# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════
@app.api_route("/cargar-ultimo", methods=["GET", "HEAD"])
@app.on_event("startup")
async def iniciar_bot():
    async for db in get_db():

        await migrar_schema(db)

        # [FIX-5] Limpiar duplicados ANTES de aplicar constraint UNIQUE
        try:
            await db.execute(text("""
                DELETE FROM auditoria_ia
                WHERE id NOT IN (
                    SELECT MIN(id) FROM auditoria_ia GROUP BY fecha, hora
                )
            """))
            await db.commit()
        except Exception:
            await db.rollback()

        # Aplicar constraint UNIQUE (ahora sin duplicados)
        try:
            await db.execute(text("""
                ALTER TABLE auditoria_ia
                ADD CONSTRAINT IF NOT EXISTS auditoria_fecha_hora_unique UNIQUE (fecha, hora)
            """))
            await db.commit()
        except Exception:
            await db.rollback()

        # V11: migrar columnas tentativo
        try:
            from app.core.scheduler import migrar_columnas_tentativo
            await migrar_columnas_tentativo(db)
        except Exception as e_mig:
            logger.warning(f"⚠️ migrar_columnas_tentativo: {e_mig}")

        # motor_pesos original
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS motor_pesos (
                    id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(),
                    peso_reciente FLOAT DEFAULT 0.30, peso_deuda FLOAT DEFAULT 0.25,
                    peso_anti FLOAT DEFAULT 0.25, peso_patron FLOAT DEFAULT 0.10,
                    peso_secuencia FLOAT DEFAULT 0.10, efectividad FLOAT DEFAULT 0.0,
                    total_evaluados INT DEFAULT 0, aciertos INT DEFAULT 0, generacion INT DEFAULT 1
                )
            """))
            res = await db.execute(text("SELECT COUNT(*) FROM motor_pesos"))
            if (res.scalar() or 0) == 0:
                await db.execute(text("""
                    INSERT INTO motor_pesos
                        (peso_reciente, peso_deuda, peso_anti, peso_patron, peso_secuencia,
                         efectividad, generacion)
                    VALUES (0.30, 0.25, 0.25, 0.10, 0.10, 4.2, 1)
                """))
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.warning(f"Warning motor_pesos: {e}")

        # V10: tablas Markov y pesos por hora
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS motor_pesos_hora (
                    hora            VARCHAR(20) NOT NULL,
                    generacion      INT NOT NULL DEFAULT 1,
                    peso_decay      FLOAT DEFAULT 0.25,
                    peso_markov     FLOAT DEFAULT 0.25,
                    peso_gap        FLOAT DEFAULT 0.25,
                    peso_reciente   FLOAT DEFAULT 0.25,
                    efectividad     FLOAT DEFAULT 0,
                    total_evaluados INT DEFAULT 0,
                    aciertos_top3   INT DEFAULT 0,
                    fecha           TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (hora, generacion)
                )
            """))
            await db.execute(text("""
                INSERT INTO motor_pesos_hora (hora, generacion) VALUES
                    ('08:00 AM',1),('09:00 AM',1),('10:00 AM',1),
                    ('11:00 AM',1),('12:00 PM',1),('01:00 PM',1),
                    ('02:00 PM',1),('03:00 PM',1),('04:00 PM',1),
                    ('05:00 PM',1),('06:00 PM',1),('07:00 PM',1)
                ON CONFLICT DO NOTHING
            """))
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS markov_transiciones (
                    id            SERIAL PRIMARY KEY,
                    hora          VARCHAR(20) NOT NULL,
                    animal_previo VARCHAR(50) NOT NULL,
                    animal_sig    VARCHAR(50) NOT NULL,
                    frecuencia    INT DEFAULT 0,
                    probabilidad  FLOAT DEFAULT 0,
                    UNIQUE(hora, animal_previo, animal_sig)
                )
            """))
            await db.commit()
            logger.info("✅ V10: markov_transiciones y motor_pesos_hora listos")
        except Exception as e:
            await db.rollback()
            logger.warning(f"Warning V10 tables: {e}")

        # Tabla aprendizaje_sorteo
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS aprendizaje_sorteo (
                    id               SERIAL PRIMARY KEY,
                    fecha            DATE NOT NULL,
                    hora             VARCHAR(20) NOT NULL,
                    animal_real      VARCHAR(50) NOT NULL,
                    animal_pred1     VARCHAR(50),
                    animal_pred2     VARCHAR(50),
                    animal_pred3     VARCHAR(50),
                    acerto_top1      BOOLEAN DEFAULT FALSE,
                    acerto_top3      BOOLEAN DEFAULT FALSE,
                    señal_dominante  VARCHAR(30),
                    peso_antes       JSONB,
                    peso_despues     JSONB,
                    delta_ef         FLOAT DEFAULT 0,
                    tasa_aprendizaje FLOAT DEFAULT 0.04,
                    generacion       INT DEFAULT 1,
                    creado           TIMESTAMP DEFAULT NOW(),
                    UNIQUE(fecha, hora)
                )
            """))
            await db.commit()
            logger.info("✅ tabla aprendizaje_sorteo lista")
        except Exception as e:
            await db.rollback()
            logger.warning(f"Warning aprendizaje_sorteo: {e}")

        break

    # V11.2: inicializar scheduler
    async with AsyncSessionLocal() as db_startup:
        await startup(db_startup)

    asyncio.create_task(ciclo_infinito())
    logger.info("🚀 LOTTOAI PRO V11 — Aprendizaje automático 12x/día ACTIVO")


# ═══════════════════════════════════════════════════════════
# HOME
# ═══════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "top3": [], "ultimos_db": [],
                "efectividad": 0, "efectividad_top3": 0,
                "aciertos_hoy": 0, "sorteos_hoy": 0,
                "total_historico": 0, "horas_rentables": [],
                "ultimo_resultado": "N/A", "analisis": "",
                "confianza_idx": 0, "señal_texto": "",
                "hora_premium": False, "ef_hora_top3": 0,
            }
        )
    except Exception as e:
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)


@app.get("/paper", response_class=HTMLResponse)
async def paper_trading(request: Request):
    paper_path = os.path.join(BASE_DIR, "paper_trading.html")
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(
            content="<h2>paper_trading.html no encontrado en la raíz del proyecto</h2>",
            status_code=404
        )


# ═══════════════════════════════════════════════════════════
# [FIX-4] /resultado — registra resultado y dispara aprendizaje
# ═══════════════════════════════════════════════════════════
@app.post("/resultado")
async def registrar_resultado(
    data: dict,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Registra el resultado real de un sorteo y dispara aprendizaje automático.
    Body JSON: {"fecha": "2026-05-18", "hora": "05:00 PM", "animal": "DELFIN"}
    """
    fecha_str = data.get("fecha")
    hora      = data.get("hora", "").strip()
    animal    = data.get("animal", "").strip()

    if not all([fecha_str, hora, animal]):
        return JSONResponse(status_code=400, content={"error": "Faltan campos: fecha, hora, animal"})

    try:
        fecha = datetime.date.fromisoformat(fecha_str)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Formato fecha inválido (usar YYYY-MM-DD)"})

    # 1. Guardar en historico si no existe
    try:
        await db.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animal, 'Lotto Activo')
            ON CONFLICT DO NOTHING
        """), {"fecha": fecha, "hora": hora, "animal": animal})
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.warning(f"Warning guardando en historico: {e}")

    # 2. Actualizar auditoria_ia
    try:
        await db.execute(text("""
            UPDATE auditoria_ia
            SET resultado_real = :animal,
                acierto = (LOWER(TRIM(prediccion_1)) = LOWER(TRIM(:animal)))
            WHERE fecha = :fecha AND hora = :hora
              AND (resultado_real IS NULL OR resultado_real = 'PENDIENTE')
        """), {"fecha": fecha, "hora": hora, "animal": animal})
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.warning(f"Warning actualizando auditoria_ia: {e}")

    # 3. Aprendizaje en background
    async def _aprender_bg():
        async with AsyncSessionLocal() as db_bg:
            resultado = await aprender_sorteo(db_bg, fecha, hora, animal)
            logger.info(
                f"🧠 Aprendizaje {fecha} {hora}: "
                f"real={animal} top1={resultado.get('acerto_top1')} "
                f"top3={resultado.get('acerto_top3')}"
            )
    background.add_task(_aprender_bg)

    return {
        "status": "ok",
        "mensaje": "✅ Resultado registrado. Aprendizaje automático iniciado.",
        "fecha": str(fecha), "hora": hora, "animal": animal,
    }


# ═══════════════════════════════════════════════════════════
# [FIX-3] /estado — pesos reales desde motor_pesos_hora
# ═══════════════════════════════════════════════════════════
@app.get("/estado")
async def estado_sistema(db: AsyncSession = Depends(get_db)):
    from zoneinfo import ZoneInfo
    from datetime import datetime
    try:
        ahora = datetime.now(ZoneInfo("America/Caracas"))

        u = (await db.execute(text(
            "SELECT fecha,hora,animalito FROM historico "
            "WHERE loteria='Lotto Activo' ORDER BY fecha DESC LIMIT 1"
        ))).fetchone()

        _h = ahora.hour
        _mn = ahora.minute
        _lbls = {
            8: "08:00 AM", 9: "09:00 AM", 10: "10:00 AM", 11: "11:00 AM",
            12: "12:00 PM", 13: "01:00 PM", 14: "02:00 PM", 15: "03:00 PM",
            16: "04:00 PM", 17: "05:00 PM", 18: "06:00 PM", 19: "07:00 PM",
        }

        if _h < 8:      _hora_prox = _lbls[8]
        elif _h >= 19:  _hora_prox = _lbls[8]
        elif _mn > 2:   _hora_prox = _lbls.get(_h + 1, _lbls[8])
        else:           _hora_prox = _lbls.get(_h, _lbls[8])

        p = (await db.execute(text("""
            SELECT fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto,
                   prediccion_1,prediccion_2,prediccion_3,
                   COALESCE(confianza_hora,0),COALESCE(es_hora_rentable,FALSE)
            FROM auditoria_ia
            WHERE fecha=:hoy AND hora=:hora
            ORDER BY fecha DESC LIMIT 1
        """), {"hoy": ahora.date(), "hora": _hora_prox})).fetchone()

        if not p:
            try:
                _pred_live = await generar_prediccion(db)
                if _pred_live and _pred_live.get("prediccion_1"):
                    try:
                        await db.execute(text("""
                            INSERT INTO auditoria_ia
                                (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                                 prediccion_3, confianza_pct, confianza_hora, es_hora_rentable)
                            VALUES
                                (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :conf_hora, :rentable)
                            ON CONFLICT (fecha, hora) DO NOTHING
                        """), {
                            "fecha": ahora.date(),
                            "hora": _pred_live.get("hora", _hora_prox),
                            "p1": _pred_live.get("prediccion_1"),
                            "p2": _pred_live.get("prediccion_2"),
                            "p3": _pred_live.get("prediccion_3"),
                            "conf": _pred_live.get("confianza_pct", 0),
                            "conf_hora": _pred_live.get("confianza_hora", 0),
                            "rentable": _pred_live.get("es_hora_rentable", False),
                        })
                        await db.commit()
                    except Exception:
                        await db.rollback()
                    p = (
                        ahora.date(), _pred_live.get("hora", _hora_prox),
                        _pred_live.get("prediccion_1"), _pred_live.get("confianza_pct", 0),
                        None, None,
                        _pred_live.get("prediccion_1"), _pred_live.get("prediccion_2"),
                        _pred_live.get("prediccion_3"),
                        _pred_live.get("confianza_hora", 0), _pred_live.get("es_hora_rentable", False),
                    )
            except Exception:
                p = (await db.execute(text("""
                    SELECT fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto,
                           prediccion_1,prediccion_2,prediccion_3,
                           COALESCE(confianza_hora,0),COALESCE(es_hora_rentable,FALSE)
                    FROM auditoria_ia ORDER BY fecha DESC LIMIT 1
                """))).fetchone()

        rh = (await db.execute(text("""
            SELECT COALESCE(SUM(total_sorteos),0),
                   COALESCE(SUM(aciertos_top1),0),
                   COALESCE(SUM(aciertos_top3),0)
            FROM rentabilidad_hora
        """))).fetchone()
        total_s = int(rh[0] or 0)
        ac1 = int(rh[1] or 0)
        ac3 = int(rh[2] or 0)
        ef1 = round(ac1 / max(total_s, 1) * 100, 2)
        ef3 = round(ac3 / max(total_s, 1) * 100, 2)

        rent = (await db.execute(text(
            "SELECT hora,efectividad_top3 FROM rentabilidad_hora "
            "WHERE es_rentable=TRUE ORDER BY efectividad_top3 DESC"
        ))).fetchall()

        hist = (await db.execute(text(
            "SELECT COUNT(*),MIN(fecha),MAX(fecha) FROM historico WHERE loteria='Lotto Activo'"
        ))).fetchone()

        markov_total = (await db.execute(text(
            "SELECT COUNT(*) FROM markov_transiciones"
        ))).scalar() or 0

        total_audit = (await db.execute(text(
            "SELECT COUNT(*) FROM auditoria_ia"
        ))).scalar() or 0

        gen = (await db.execute(text(
            "SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"
        ))).scalar() or 1

        pesos_hora_row = (await db.execute(text("""
            SELECT peso_decay, peso_markov, peso_gap, peso_reciente, efectividad
            FROM motor_pesos_hora
            WHERE hora = :hora
            ORDER BY generacion DESC LIMIT 1
        """), {"hora": _hora_prox})).fetchone()

        pesos_reales = {
            "decay":    round(float(pesos_hora_row[0]), 3) if pesos_hora_row else 0.25,
            "markov":   round(float(pesos_hora_row[1]), 3) if pesos_hora_row else 0.25,
            "gap":      round(float(pesos_hora_row[2]), 3) if pesos_hora_row else 0.25,
            "reciente": round(float(pesos_hora_row[3]), 3) if pesos_hora_row else 0.25,
        }

        aprendizaje_row = (await db.execute(text("""
            SELECT tasa_aprendizaje FROM aprendizaje_sorteo
            WHERE hora = :hora
            ORDER BY creado DESC LIMIT 1
        """), {"hora": _hora_prox})).fetchone()

        lambda_actual = round(float(aprendizaje_row[0]) if aprendizaje_row else 0.008, 4)

        aprendizaje_hoy = (await db.execute(text("""
            SELECT COUNT(*), SUM(CASE WHEN acerto_top3 THEN 1 ELSE 0 END)
            FROM aprendizaje_sorteo WHERE fecha = :hoy
        """), {"hoy": ahora.date()})).fetchone()

        return {
            "estado": "✅ SISTEMA ACTIVO — Motor V11 Auto-Learning",
            "hora_venezolana": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "motor": {
                "version": "V11",
                "generacion": gen,
                "markov_transiciones": int(markov_total),
                "decay_lambda": lambda_actual,
                "pesos_hora_actual": pesos_reales,
                "aprendizaje_hoy": {
                    "sorteos_aprendidos": int(aprendizaje_hoy[0] or 0),
                    "aciertos_top3": int(aprendizaje_hoy[1] or 0),
                }
            },
            "ultimo_capturado": {
                "fecha": str(u[0]), "hora": u[1], "animal": u[2]
            } if u else {},
            "ultima_prediccion": {
                "fecha": str(p[0]), "hora": p[1],
                "pred1": p[6], "pred2": p[7], "pred3": p[8],
                "confianza": round(float(p[3] or 0)),
                "confianza_hora": round(float(p[9] or 0), 1),
                "es_hora_rentable": bool(p[10]),
                "real": p[4], "acierto": p[5],
            } if p else {},
            "metricas": {
                "total": int(total_audit), "calibradas": total_s,
                "aciertos_top1": ac1, "aciertos_top3": ac3,
                "efectividad_top1": ef1, "efectividad_top3": ef3,
            },
            "historico": {
                "total": int(hist[0] or 0),
                "desde": str(hist[1]), "hasta": str(hist[2]),
            },
            "horas_rentables": [{"hora": r[0], "ef_top3": float(r[1])} for r in rent],
            "total_db": int(total_audit),
            "total_calibrados": total_s,
            "aciertos_top1": ac1,
            "aciertos_top3": ac3,
            "efectividad_top1": ef1,
            "efectividad_top3": ef3,
            "horas_rentables_n": len(rent),
            "prediccion_actual": {
                "animal_predicho": p[2] if p else None,
                "prediccion_1": p[6] if p else None,
                "prediccion_2": p[7] if p else None,
                "prediccion_3": p[8] if p else None,
                "hora": p[1] if p else None,
                "confianza_pct": round(float(p[3] or 0)) if p else 0,
                "confianza_hora": round(float(p[9] or 0), 1) if p else 0,
                "es_hora_rentable": bool(p[10]) if p else False,
                "acierto": p[5] if p else None,
            } if p else None,
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {
            "estado": f"❌ ERROR /estado: {str(e)}",
            "total_db": 0, "total_calibrados": 0,
            "aciertos_top1": 0, "aciertos_top3": 0,
            "efectividad_top1": 0.0, "efectividad_top3": 0.0,
            "horas_rentables": [], "prediccion_actual": None,
            "horas_rentables_n": 0,
        }


# ═══════════════════════════════════════════════════════════
# ULTIMOS
# ═══════════════════════════════════════════════════════════
@app.get("/ultimos")
async def ultimos(limit: int = Query(default=15), db: AsyncSession = Depends(get_db)):
    try:
        rows = (await db.execute(text("""
            SELECT fecha, hora, animal_predicho,
                   prediccion_1, prediccion_2, prediccion_3,
                   confianza_pct, confianza_hora, es_hora_rentable,
                   acierto, resultado_real,
                   pred_tentativa_1, pred_tentativa_2, pred_tentativa_3,
                   origen
            FROM auditoria_ia
            ORDER BY fecha DESC,
            CASE hora
                WHEN '08:00 AM' THEN 8  WHEN '09:00 AM' THEN 9
                WHEN '10:00 AM' THEN 10 WHEN '11:00 AM' THEN 11
                WHEN '12:00 PM' THEN 12 WHEN '01:00 PM' THEN 13
                WHEN '02:00 PM' THEN 14 WHEN '03:00 PM' THEN 15
                WHEN '04:00 PM' THEN 16 WHEN '05:00 PM' THEN 17
                WHEN '06:00 PM' THEN 18 WHEN '07:00 PM' THEN 19
                ELSE 0 END DESC
            LIMIT :limit
        """), {"limit": limit})).fetchall()

        return [
            {
                "fecha": str(r[0]),
                "hora": r[1],
                "animal_predicho": r[2],
                "prediccion_1": r[3],
                "prediccion_2": r[4],
                "prediccion_3": r[5],
                "confianza_pct": float(r[6]) if r[6] else None,
                "confianza_hora": float(r[7]) if r[7] else None,
                "es_hora_rentable": bool(r[8]) if r[8] is not None else False,
                "acierto": bool(r[9]) if r[9] is not None else None,
                "resultado_real": r[10],
                "pred_tentativa_1": r[11],
                "pred_tentativa_2": r[12],
                "pred_tentativa_3": r[13],
                "origen": r[14] or "INICIAL",
            }
            for r in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════
@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    from zoneinfo import ZoneInfo
    ahora = datetime.datetime.now(ZoneInfo("America/Caracas"))
    hoy = ahora.date()
    checks = {}

    try:
        await db.execute(text("SELECT 1"))
        checks["bd_conectada"] = {"ok": True, "msg": "✅ Neon conectado"}
    except Exception as e:
        checks["bd_conectada"] = {"ok": False, "msg": f"❌ {e}"}

    mk_hoy = (await db.execute(text(
        "SELECT COUNT(*) FROM markov_transiciones WHERE frecuencia > 0"
    ))).scalar() or 0
    checks["markov"] = {
        "ok": mk_hoy > 1000,
        "msg": f"{'✅' if mk_hoy > 1000 else '⚠️'} {mk_hoy:,} transiciones"
    }

    pesos_distintos = (await db.execute(text("""
        SELECT COUNT(*) FROM motor_pesos_hora
        WHERE peso_decay != 0.25 OR peso_markov != 0.25
    """))).scalar() or 0
    checks["pesos_adaptados"] = {
        "ok": pesos_distintos > 0,
        "msg": f"{'✅' if pesos_distintos > 0 else '❌ PESOS AÚN EN 0.25'} "
               f"{pesos_distintos} horas con pesos adaptados"
    }

    ap_hoy = (await db.execute(text("""
        SELECT COUNT(*), COALESCE(AVG(CASE WHEN acerto_top3 THEN 100.0 ELSE 0 END), 0)
        FROM aprendizaje_sorteo WHERE fecha = :hoy
    """), {"hoy": hoy})).fetchone()
    checks["aprendizaje_hoy"] = {
        "ok": int(ap_hoy[0] or 0) > 0,
        "msg": f"{'✅' if (ap_hoy[0] or 0) > 0 else '⚠️ Sin aprendizaje hoy'} "
               f"{int(ap_hoy[0] or 0)} sorteos aprendidos, "
               f"ef_hoy={round(float(ap_hoy[1] or 0), 1)}%"
    }

    preds_hoy = (await db.execute(text(
        "SELECT COUNT(*) FROM auditoria_ia WHERE fecha = :hoy AND prediccion_1 IS NOT NULL"
    ), {"hoy": hoy})).scalar() or 0
    checks["predicciones_hoy"] = {
        "ok": preds_hoy > 0,
        "msg": f"{'✅' if preds_hoy > 0 else '⚠️'} {preds_hoy} predicciones para hoy"
    }

    ef_global = (await db.execute(text("""
        SELECT ROUND(AVG(efectividad_top3)::numeric, 2)
        FROM rentabilidad_hora WHERE total_sorteos > 10
    """))).scalar() or 0
    checks["efectividad_global"] = {
        "ok": float(ef_global) >= 10.0,
        "msg": f"{'✅' if float(ef_global) >= 10 else '⚠️'} ef_top3 promedio = {ef_global}%"
    }

    todo_ok = all(c["ok"] for c in checks.values())
    return {
        "status": "✅ SANO" if todo_ok else "⚠️ REVISAR",
        "hora": ahora.strftime("%H:%M:%S"),
        "checks": checks
    }


# ═══════════════════════════════════════════════════════════
# PREDECIR
# ═══════════════════════════════════════════════════════════
@app.get("/predecir")
async def predecir_hora(hora: str = Query(default=None), db: AsyncSession = Depends(get_db)):
    try:
        if not hora:
            r = (await db.execute(text(
                "SELECT hora FROM auditoria_ia ORDER BY fecha DESC LIMIT 1"
            ))).fetchone()
            hora = r[0] if r else "11:00 AM"

        p = (await db.execute(text("""
            SELECT fecha, hora, animal_predicho,
                   prediccion_1, prediccion_2, prediccion_3,
                   confianza_pct, confianza_hora, es_hora_rentable,
                   acierto, resultado_real
            FROM auditoria_ia
            WHERE hora = :hora
            ORDER BY fecha DESC LIMIT 1
        """), {"hora": hora})).fetchone()

        try:
            from datetime import datetime as _dt
            _h = hora.strip().upper().replace(" ", " ")
            if " " not in _h:
                _h = _h[:-2] + " " + _h[-2:]
            hora = _dt.strptime(_h, "%I:%M %p").strftime("%I:%M %p")
        except Exception:
            pass

        rent = (await db.execute(text("""
            SELECT efectividad_top1, efectividad_top3, es_rentable, total_sorteos
            FROM rentabilidad_hora WHERE TRIM(hora) = TRIM(:hora)
        """), {"hora": hora})).fetchone()

        pw = (await db.execute(text("""
            SELECT peso_decay, peso_markov, peso_gap, peso_reciente, efectividad
            FROM motor_pesos_hora
            WHERE hora = :hora
            ORDER BY generacion DESC LIMIT 1
        """), {"hora": hora})).fetchone()

        if not pw or (pw[0] == pw[1] == pw[2] == pw[3]):
            top3_prob = (await db.execute(text("""
                SELECT animalito, probabilidad FROM probabilidades_hora
                WHERE hora=:hora ORDER BY probabilidad DESC LIMIT 3
            """), {"hora": hora})).fetchall()
            mk_count = (await db.execute(text("""
                SELECT COUNT(*) FROM markov_transiciones WHERE hora=:hora
            """), {"hora": hora})).fetchone()
            ef3 = float(rent[1]) if rent and rent[1] else 0
            p_decay  = 0.30 if top3_prob and float(top3_prob[0][1] or 0) > 3.5 else 0.25
            p_markov = 0.30 if mk_count and int(mk_count[0] or 0) > 50 else 0.20
            p_gap    = 0.25
            p_rec    = round(1.0 - p_decay - p_markov - p_gap, 2)
            pw = (round(p_decay, 2), round(p_markov, 2), round(p_gap, 2), p_rec, ef3)

        prev = (await db.execute(text("""
            SELECT animalito FROM historico
            WHERE loteria='Lotto Activo' AND hora=:hora
            ORDER BY fecha DESC OFFSET 1 LIMIT 1
        """), {"hora": hora})).fetchone()
        animal_previo = prev[0] if prev else None

        markov_top = []
        if animal_previo:
            mk = (await db.execute(text("""
                SELECT animal_sig, ROUND(probabilidad::numeric, 2) AS prob_pct
                FROM markov_transiciones
                WHERE hora=:hora
                  AND LOWER(TRIM(animal_previo))=LOWER(TRIM(:animal))
                  AND frecuencia >= 3
                ORDER BY probabilidad DESC LIMIT 3
            """), {"hora": hora, "animal": animal_previo})).fetchall()
            markov_top = [{"animal": r[0], "prob_pct": float(r[1])} for r in mk]

        return {
            "status": "success",
            "hora": hora,
            "prediccion_1": p[3] if p else None,
            "prediccion_2": p[4] if p else None,
            "prediccion_3": p[5] if p else None,
            "animal_predicho": p[2] if p else None,
            "confianza_pct": float(p[6]) if p and p[6] else 0,
            "confianza_hora": float(p[7]) if p and p[7] else 0,
            "es_hora_rentable": bool(p[8]) if p and p[8] is not None else False,
            "ef_top1": float(rent[0]) if rent else 0,
            "ef_top3": float(rent[1]) if rent else 0,
            "total_sorteos": int(rent[3]) if rent else 0,
            "markov_signal": {"animal_previo": animal_previo, "top3": markov_top},
            "pesos_hora": {
                "peso_decay":    float(pw[0]) if pw else 0.25,
                "peso_markov":   float(pw[1]) if pw else 0.25,
                "peso_gap":      float(pw[2]) if pw else 0.25,
                "peso_reciente": float(pw[3]) if pw else 0.25,
                "efectividad":   float(pw[4]) if pw else 0,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ═══════════════════════════════════════════════════════════
# RECALIBRAR MANUAL
# ═══════════════════════════════════════════════════════════
@app.api_route("/recalibrar-pesos", methods=["GET", "POST"])
async def recalibrar_pesos_manual(
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    async def _bg():
        async with AsyncSessionLocal() as db_bg:
            r = await recalcular_todos_los_pesos(db_bg)
            logger.info(f"✅ Recalibración manual completada: {r['ok']}/{r['total']}")
    background.add_task(_bg)
    return {
        "status": "ok",
        "mensaje": "✅ Recalibración de pesos iniciada en background. "
                   "Completará en ~30 segundos. Ver /health para verificar."
    }


# ═══════════════════════════════════════════════════════════
# [FIX-8] /recuperar-pendientes — cruza historico con auditoria_ia
# ═══════════════════════════════════════════════════════════
@app.get("/recuperar-pendientes")
async def recuperar_pendientes(
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Cruza auditoria_ia (resultado_real = PENDIENTE) con historico.
    Para cada coincidencia:
      1. Actualiza resultado_real y calcula acierto en auditoria_ia
      2. Actualiza rentabilidad_hora
      3. Inserta en aprendizaje_sorteo
    Ejecuta en background — responde inmediatamente.
    """
    pendientes = (await db.execute(text("""
        SELECT COUNT(*) FROM auditoria_ia
        WHERE resultado_real = 'PENDIENTE' OR resultado_real IS NULL
    """))).scalar() or 0

    async def _recuperar_bg():
        async with AsyncSessionLocal() as db_bg:
            try:
                rows = (await db_bg.execute(text("""
                    SELECT
                        a.fecha, a.hora,
                        a.prediccion_1, a.prediccion_2, a.prediccion_3,
                        h.animalito AS real
                    FROM auditoria_ia a
                    JOIN historico h
                      ON h.fecha = a.fecha
                     AND TRIM(h.hora) = TRIM(a.hora)
                     AND h.loteria = 'Lotto Activo'
                    WHERE (a.resultado_real = 'PENDIENTE' OR a.resultado_real IS NULL)
                      AND a.prediccion_1 IS NOT NULL
                    ORDER BY a.fecha ASC, a.hora ASC
                """))).fetchall()

                logger.info(f"🔄 recuperar-pendientes: {len(rows)} sorteos con match en historico")

                recuperados = 0
                aciertos_top1 = 0
                aciertos_top3 = 0

                for row in rows:
                    fecha, hora, p1, p2, p3, real = row
                    real_l = (real or "").lower().strip()
                    p1_l   = (p1  or "").lower().strip()
                    p2_l   = (p2  or "").lower().strip()
                    p3_l   = (p3  or "").lower().strip()

                    ac1 = real_l == p1_l
                    ac3 = real_l in (p1_l, p2_l, p3_l)

                    try:
                        # 1. Actualizar auditoria_ia
                        await db_bg.execute(text("""
                            UPDATE auditoria_ia
                            SET resultado_real = :real,
                                acierto = :ac1
                            WHERE fecha = :fecha AND hora = :hora
                              AND (resultado_real = 'PENDIENTE' OR resultado_real IS NULL)
                        """), {"real": real_l, "ac1": ac1, "fecha": fecha, "hora": hora})

                        # 2. Actualizar rentabilidad_hora
                        await db_bg.execute(text("""
                            INSERT INTO rentabilidad_hora
                                (hora, total_sorteos, aciertos_top1, aciertos_top3,
                                 efectividad_top1, efectividad_top3, es_rentable)
                            VALUES (:hora, 1, :ac1, :ac3, 0, 0, FALSE)
                            ON CONFLICT (hora) DO UPDATE SET
                                total_sorteos    = rentabilidad_hora.total_sorteos + 1,
                                aciertos_top1    = rentabilidad_hora.aciertos_top1 + :ac1,
                                aciertos_top3    = rentabilidad_hora.aciertos_top3 + :ac3,
                                efectividad_top1 = ROUND(
                                    (rentabilidad_hora.aciertos_top1 + :ac1)::numeric /
                                    NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100, 2),
                                efectividad_top3 = ROUND(
                                    (rentabilidad_hora.aciertos_top3 + :ac3)::numeric /
                                    NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100, 2),
                                es_rentable = (
                                    (rentabilidad_hora.aciertos_top3 + :ac3)::float /
                                    NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100
                                ) >= 10.0,
                                updated_at = NOW()
                        """), {"hora": hora, "ac1": int(ac1), "ac3": int(ac3)})

                        # 3. Insertar en aprendizaje_sorteo
                        await db_bg.execute(text("""
                            INSERT INTO aprendizaje_sorteo
                                (fecha, hora, animal_real, animal_pred1, animal_pred2,
                                 animal_pred3, acerto_top1, acerto_top3,
                                 señal_dominante, tasa_aprendizaje, generacion)
                            VALUES
                                (:fecha, :hora, :real, :p1, :p2, :p3,
                                 :ac1, :ac3, 'recuperado', 0.04, 1)
                            ON CONFLICT (fecha, hora) DO UPDATE SET
                                animal_real = :real,
                                acerto_top1 = :ac1,
                                acerto_top3 = :ac3
                        """), {
                            "fecha": fecha, "hora": hora,
                            "real": real_l,
                            "p1": p1_l, "p2": p2_l, "p3": p3_l,
                            "ac1": ac1, "ac3": ac3,
                        })

                        await db_bg.commit()
                        recuperados += 1
                        if ac1: aciertos_top1 += 1
                        if ac3: aciertos_top3 += 1

                    except Exception as e_row:
                        await db_bg.rollback()
                        logger.warning(f"⚠️ Error recuperando {fecha} {hora}: {e_row}")

                ef1 = round(aciertos_top1 / max(recuperados, 1) * 100, 2)
                ef3 = round(aciertos_top3 / max(recuperados, 1) * 100, 2)
                logger.info(
                    f"✅ Recuperación completa: {recuperados} sorteos — "
                    f"ef_top1={ef1}% ef_top3={ef3}%"
                )

            except Exception as e_bg:
                logger.error(f"❌ Error en _recuperar_bg: {e_bg}")

    background.add_task(_recuperar_bg)

    return {
        "status": "ok",
        "pendientes_encontrados": int(pendientes),
        "mensaje": (
            f"✅ Recuperación iniciada para {pendientes} sorteos PENDIENTE. "
            f"Completa en ~30s. Verifica con /health o /ultimos."
        ),
    }


# ═══════════════════════════════════════════════════════════
# HISTORIAL, MARKOV y demás endpoints
# ═══════════════════════════════════════════════════════════
@app.get("/historial")
async def get_historial(
    limit: int = Query(default=50),
    offset: int = Query(default=0),
    fecha: str = Query(default=None),
    resultado: str = Query(default=None),
    animal: str = Query(default=None),
    db: AsyncSession = Depends(get_db)
):
    try:
        conditions = []
        params: dict = {"limit": limit, "offset": offset}
        if fecha:
            conditions.append("a.fecha = :fecha")
            params["fecha"] = fecha
        if resultado == "true":
            conditions.append("a.acierto = TRUE")
        elif resultado == "false":
            conditions.append("a.acierto = FALSE")
        if animal:
            an = animal.lower().strip()
            conditions.append(
                "(LOWER(a.prediccion_1)=:an OR LOWER(a.prediccion_2)=:an "
                "OR LOWER(a.prediccion_3)=:an OR LOWER(a.resultado_real)=:an)"
            )
            params["an"] = an
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = (await db.execute(text(f"""
            SELECT a.fecha, a.hora, a.prediccion_1, a.prediccion_2, a.prediccion_3,
                   a.resultado_real, a.acierto, a.confianza_pct
            FROM auditoria_ia a
            {where}
            ORDER BY a.fecha DESC, a.hora DESC
            LIMIT :limit OFFSET :offset
        """), params)).fetchall()
        return {
            "registros": [{
                "fecha": str(r[0]), "hora": r[1],
                "prediccion_1": r[2] or "", "prediccion_2": r[3] or "",
                "prediccion_3": r[4] or "",
                "resultado_real": r[5] or "PENDIENTE",
                "acierto": r[6], "confianza_pct": int(r[7] or 0),
            } for r in rows],
            "total": len(rows),
            "offset": offset,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/actualizar-señales")
async def actualizar_senales(db: AsyncSession = Depends(get_db)):
    try:
        res = await actualizar_resultados_señales(db)
        return {"status": "ok", "resultado": res}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/diagnostico-historico")
async def diagnostico_historico(db: AsyncSession = Depends(get_db)):
    try:
        cols = (await db.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'historico'
            ORDER BY ordinal_position
        """))).fetchall()
        muestra = (await db.execute(text(
            "SELECT * FROM historico WHERE loteria='Lotto Activo' ORDER BY fecha DESC LIMIT 3"
        ))).fetchall()
        keys = (await db.execute(text(
            "SELECT * FROM historico WHERE loteria='Lotto Activo' ORDER BY fecha DESC LIMIT 1"
        ))).keys()
        return {
            "columnas": [{"nombre": c[0], "tipo": c[1]} for c in cols],
            "columnas_keys": list(keys),
            "muestra": [list(r) for r in muestra]
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/markov/top")
async def markov_top(limit: int = Query(default=20), db: AsyncSession = Depends(get_db)):
    try:
        rows = (await db.execute(text("""
            SELECT hora, animal_previo, animal_sig,
                   frecuencia,
                   CASE
                       WHEN probabilidad > 100
                       THEN ROUND((frecuencia::FLOAT /
                            NULLIF(SUM(frecuencia) OVER (PARTITION BY hora, animal_previo), 0)
                            * 100)::numeric, 2)
                       ELSE ROUND(probabilidad::numeric, 2)
                   END AS probabilidad_pct,
                   SUM(frecuencia) OVER (PARTITION BY hora, animal_previo) AS total_desde_previo
            FROM markov_transiciones
            WHERE frecuencia >= 5
        """))).fetchall()
        filtrados = [r for r in rows if (r[5] or 0) >= 20]
        filtrados.sort(key=lambda r: float(r[4] or 0), reverse=True)
        filtrados = filtrados[:limit]
        return [{
            "hora": r[0], "animal_previo": r[1], "animal_sig": r[2],
            "frecuencia": int(r[3]),
            "probabilidad_pct": float(r[4]) if r[4] else 0,
            "total_desde_previo": int(r[5] or 0),
        } for r in filtrados]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/markov")
async def markov_buscar(
    hora: str = Query(default="08:00 AM"),
    animal: str = Query(default=""),
    db: AsyncSession = Depends(get_db)
):
    try:
        animal_norm = animal.lower().strip()
        if not animal_norm:
            return []
        rows = (await db.execute(text("""
            SELECT animal_previo, animal_sig, frecuencia,
                   CASE
                       WHEN probabilidad > 100
                       THEN ROUND((frecuencia::FLOAT /
                            NULLIF(SUM(frecuencia) OVER (PARTITION BY hora, animal_previo), 0)
                            * 100)::numeric, 2)
                       ELSE ROUND(probabilidad::numeric, 2)
                   END AS prob
            FROM markov_transiciones
            WHERE hora = :hora AND LOWER(animal_previo) = :animal
            ORDER BY frecuencia DESC LIMIT 15
        """), {"hora": hora, "animal": animal_norm})).fetchall()
        return [{
            "animal_previo": r[0], "animal_sig": r[1],
            "frecuencia": int(r[2]), "probabilidad_pct": float(r[3]) if r[3] else 0,
        } for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/fix-markov-directo")
async def fix_markov_directo(db: AsyncSession = Depends(get_db)):
    try:
        antes = (await db.execute(text(
            "SELECT COUNT(*), MAX(probabilidad) FROM markov_transiciones WHERE probabilidad > 100"
        ))).fetchone()
        n_corruptas = int(antes[0] or 0)
        prob_max_antes = float(antes[1] or 0)

        if n_corruptas == 0:
            total = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0
            return {
                "status": "ok",
                "mensaje": f"✅ Ya estaba limpio. {total:,} transiciones válidas.",
                "corruptas_antes": 0,
            }

        await db.execute(text("""
            UPDATE markov_transiciones m
            SET probabilidad = ROUND(
                (m.frecuencia::FLOAT /
                 NULLIF(sub.total_prev, 0) * 100)::numeric, 2
            )
            FROM (
                SELECT hora, animal_previo, SUM(frecuencia) AS total_prev
                FROM markov_transiciones
                GROUP BY hora, animal_previo
            ) sub
            WHERE m.hora = sub.hora AND m.animal_previo = sub.animal_previo
        """))
        await db.commit()

        despues = (await db.execute(text(
            "SELECT COUNT(*), MAX(probabilidad) FROM markov_transiciones WHERE probabilidad > 100"
        ))).fetchone()
        n_restantes = int(despues[0] or 0)

        if n_restantes > 0:
            await db.execute(text("DELETE FROM markov_transiciones WHERE probabilidad > 100"))
            await db.commit()

        total = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0
        prob_max = (await db.execute(text("SELECT MAX(probabilidad) FROM markov_transiciones"))).scalar() or 0

        return {
            "status": "success",
            "corruptas_antes": n_corruptas,
            "prob_max_antes": prob_max_antes,
            "prob_max_ahora": float(prob_max),
            "total_transiciones": total,
            "mensaje": f"✅ Corregidas {n_corruptas} filas. Prob máx ahora: {float(prob_max):.2f}%",
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "mensaje": str(e)}
