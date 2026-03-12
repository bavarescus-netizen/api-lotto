import os, re, asyncio, datetime
from fastapi import FastAPI, Request, Depends, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db, AsyncSessionLocal
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito
from app.services.motor_v10 import (
    generar_prediccion, obtener_estadisticas, obtener_bitacora,
    entrenar_modelo, backtest, calibrar_predicciones,
    llenar_auditoria_retroactiva, aprender_desde_historico,
    migrar_schema,
)

# ── Estado global de tareas largas (no bloquean el servidor) ──
_tarea = {
    "nombre": None,
    "estado": "idle",    # idle | running | done | error
    "progreso": "",
    "resultado": None,
    "iniciado": None,
}

async def _run_aprender(fecha_inicio):
    _tarea.update({"nombre":"aprender","estado":"running",
                   "progreso":"Iniciando...","resultado":None,
                   "iniciado": str(datetime.datetime.now())})
    try:
        async with AsyncSessionLocal() as db:
            res = await aprender_desde_historico(db, fecha_inicio)
        _tarea.update({"estado":"done","progreso":"Completado","resultado":res})
    except Exception as e:
        _tarea.update({"estado":"error","progreso":str(e)})

async def _run_retroactivo(fd, fh, dias):
    _tarea.update({"nombre":"retroactivo","estado":"running",
                   "progreso":"Iniciando retroactivo...","resultado":None,
                   "iniciado": str(datetime.datetime.now())})
    try:
        async with AsyncSessionLocal() as db:
            res = await llenar_auditoria_retroactiva(db, fd, fh, dias)
        _tarea.update({"estado":"done","progreso":"Completado","resultado":res})
    except Exception as e:
        _tarea.update({"estado":"error","progreso":str(e)})

app = FastAPI(title="LOTTOAI PRO V10")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
    allow_credentials=False, allow_methods=["GET","POST"], allow_headers=["*"])
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(prediccion.router)
app.include_router(cargarhist.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR,"imagenes")), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR,"app","routes"))


# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════
@app.on_event("startup")
async def iniciar_bot():
    async for db in get_db():
        await migrar_schema(db)
        try:
            await db.execute(text("""
                ALTER TABLE auditoria_ia
                ADD CONSTRAINT IF NOT EXISTS auditoria_fecha_hora_unique UNIQUE (fecha,hora)
            """))
            await db.commit()
        except Exception:
            await db.rollback()

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
                        (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia,efectividad,generacion)
                    VALUES (0.30,0.25,0.25,0.10,0.10,4.2,1)
                """))
            await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"Warning motor_pesos: {e}")

        # V10: tablas nuevas
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS motor_pesos_hora (
                    hora           VARCHAR(20) NOT NULL,
                    generacion     INT         NOT NULL DEFAULT 1,
                    peso_decay     FLOAT       DEFAULT 0.25,
                    peso_markov    FLOAT       DEFAULT 0.25,
                    peso_gap       FLOAT       DEFAULT 0.25,
                    peso_reciente  FLOAT       DEFAULT 0.25,
                    efectividad    FLOAT       DEFAULT 0,
                    total_evaluados INT        DEFAULT 0,
                    aciertos_top3  INT         DEFAULT 0,
                    fecha          TIMESTAMP   DEFAULT NOW(),
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
                    id             SERIAL PRIMARY KEY,
                    hora           VARCHAR(20) NOT NULL,
                    animal_previo  VARCHAR(50) NOT NULL,
                    animal_sig     VARCHAR(50) NOT NULL,
                    frecuencia     INT DEFAULT 0,
                    probabilidad   FLOAT DEFAULT 0,
                    UNIQUE(hora, animal_previo, animal_sig)
                )
            """))
            await db.commit()
            print("✅ V10: markov_transiciones y motor_pesos_hora listos")
        except Exception as e:
            await db.rollback()
            print(f"Warning V10 tables: {e}")

        break
    asyncio.create_task(ciclo_infinito())
    print("🚀 LOTTOAI PRO V10 — Markov + Decay + Gap + Pesos por hora")


# ═══════════════════════════════════════════════════════════
# HOME — Dashboard Jinja2
# ═══════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia     = await generar_prediccion(db)
        stats_data = await obtener_estadisticas(db)
        res_db = await db.execute(text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto, a.animal_predicho,
                   a.prediccion_1, a.prediccion_2, a.prediccion_3
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha=a.fecha AND h.hora=a.hora
            WHERE h.loteria='Lotto Activo'
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """))
        ultimos_db = []
        for r in res_db.fetchall():
            nombre_animal = re.sub(r'[^a-z]','',r[2].lower())
            predicho_raw  = re.sub(r'[^a-z]','',(r[4] or '').lower())
            fecha_str     = r[0].strftime("%m-%d") if r[0] else "—"
            ultimos_db.append({
                "fecha": fecha_str, "hora": r[1], "animal": r[2],
                "img": f"{nombre_animal}.png", "acierto": r[3],
                "predicho": predicho_raw,
                "prediccion_1": r[5], "prediccion_2": r[6], "prediccion_3": r[7],
            })
        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3",[]),
            "ultimos_db": ultimos_db,
            "efectividad": stats_data.get("efectividad_global",0),
            "efectividad_top3": stats_data.get("efectividad_top3",0),
            "aciertos_hoy": stats_data.get("aciertos_hoy",0),
            "sorteos_hoy": stats_data.get("sorteos_hoy",0),
            "total_historico": stats_data.get("total_historico",0),
            "horas_rentables": stats_data.get("horas_rentables",[]),
            "ultimo_resultado": res_ia.get("ultimo_resultado","N/A"),
            "analisis": res_ia.get("analisis",""),
            "confianza_idx": res_ia.get("confianza_idx",0),
            "señal_texto": res_ia.get("señal_texto",""),
            "hora_premium": res_ia.get("hora_premium",False),
            "ef_hora_top3": res_ia.get("efectividad_hora_top3",0),
        })
    except Exception as e:
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)


# ═══════════════════════════════════════════════════════════
# ESTADO — V10 completo
# ═══════════════════════════════════════════════════════════
@app.get("/estado")
async def estado_sistema(db: AsyncSession = Depends(get_db)):
    import pytz
    from datetime import datetime
    try:
        ahora = datetime.now(pytz.timezone('America/Caracas'))

        # ── Último resultado capturado ──
        u = (await db.execute(text(
            "SELECT fecha,hora,animalito FROM historico "
            "WHERE loteria='Lotto Activo' ORDER BY fecha DESC LIMIT 1"
        ))).fetchone()

        # ── Última predicción ──
        p = (await db.execute(text(
            "SELECT fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto,"
            "prediccion_1,prediccion_2,prediccion_3,"
            "COALESCE(confianza_hora,0),COALESCE(es_hora_rentable,FALSE) "
            "FROM auditoria_ia ORDER BY fecha DESC LIMIT 1"
        ))).fetchone()

        # ── Métricas desde rentabilidad_hora (sin JOINs pesados) ──
        rh = (await db.execute(text("""
            SELECT
                COALESCE(SUM(total_sorteos),0)  AS total,
                COALESCE(SUM(aciertos_top1),0)  AS ac1,
                COALESCE(SUM(aciertos_top3),0)  AS ac3
            FROM rentabilidad_hora
        """))).fetchone()
        total_s = int(rh[0] or 0)
        ac1     = int(rh[1] or 0)
        ac3     = int(rh[2] or 0)
        ef1     = round(ac1 / max(total_s, 1) * 100, 2)
        ef3     = round(ac3 / max(total_s, 1) * 100, 2)

        # ── Horas rentables ──
        rent = (await db.execute(text(
            "SELECT hora,efectividad_top3 FROM rentabilidad_hora "
            "WHERE es_rentable=TRUE ORDER BY efectividad_top3 DESC"
        ))).fetchall()

        # ── Total historico ──
        hist = (await db.execute(text(
            "SELECT COUNT(*),MIN(fecha),MAX(fecha) FROM historico WHERE loteria='Lotto Activo'"
        ))).fetchone()

        # ── Markov ──
        markov_total = (await db.execute(text(
            "SELECT COUNT(*) FROM markov_transiciones"
        ))).scalar() or 0

        # ── Total auditoria ──
        total_audit = (await db.execute(text(
            "SELECT COUNT(*) FROM auditoria_ia"
        ))).scalar() or 0

        # ── Generación motor ──
        gen = (await db.execute(text(
            "SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"
        ))).scalar() or 1

        hr = len(rent)

        return {
            "estado": "✅ SISTEMA ACTIVO — Motor V10",
            "hora_venezolana": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "motor": {
                "version": "V10", "generacion": gen,
                "markov_transiciones": int(markov_total),
                "decay_lambda": 0.008,
                "pesos": {"reciente":0.25,"deuda":0.25,"anti":0.25,"patron":0.25},
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
            # ── Aliases planos para el dashboard JS ──
            "total_db":          int(total_audit),
            "total_calibrados":  total_s,
            "aciertos_top1":     ac1,
            "aciertos_top3":     ac3,
            "efectividad_top1":  ef1,
            "efectividad_top3":  ef3,
            "horas_rentables_n": hr,
            "prediccion_actual": {
                "animal_predicho":  p[2] if p else None,
                "prediccion_1":     p[6] if p else None,
                "prediccion_2":     p[7] if p else None,
                "prediccion_3":     p[8] if p else None,
                "hora":             p[1] if p else None,
                "confianza_pct":    round(float(p[3] or 0)) if p else 0,
                "confianza_hora":   round(float(p[9] or 0), 1) if p else 0,
                "es_hora_rentable": bool(p[10]) if p else False,
                "acierto":          p[5] if p else None,
            } if p else None,
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {
            "estado":           f"❌ ERROR /estado: {str(e)}",
            "total_db":          0, "total_calibrados": 0,
            "aciertos_top1":     0, "aciertos_top3":    0,
            "efectividad_top1":  0.0, "efectividad_top3": 0.0,
            "horas_rentables":   [], "prediccion_actual": None,
            "horas_rentables_n": 0,
        }


# ═══════════════════════════════════════════════════════════
# ULTIMOS — con pred1/pred2/pred3
# ═══════════════════════════════════════════════════════════
@app.get("/ultimos")
async def ultimos(limit: int = Query(default=10), db: AsyncSession = Depends(get_db)):
    try:
        rows = (await db.execute(text("""
            SELECT fecha, hora, animal_predicho,
                   prediccion_1, prediccion_2, prediccion_3,
                   confianza_pct, confianza_hora, es_hora_rentable,
                   acierto, resultado_real
            FROM auditoria_ia
            ORDER BY fecha DESC, hora DESC
            LIMIT :limit
        """), {"limit": limit})).fetchall()
        return [
            {
                "fecha":            str(r[0]),
                "hora":             r[1],
                "animal_predicho":  r[2],
                "prediccion_1":     r[3],
                "prediccion_2":     r[4],
                "prediccion_3":     r[5],
                "confianza_pct":    float(r[6]) if r[6] else None,
                "confianza_hora":   float(r[7]) if r[7] else None,
                "es_hora_rentable": bool(r[8]) if r[8] is not None else False,
                "acierto":          bool(r[9]) if r[9] is not None else None,
                "resultado_real":   r[10],
            }
            for r in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# PREDECIR — V10 por hora con señal Markov
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

        pw = (await db.execute(text("""
            SELECT peso_decay, peso_markov, peso_gap, peso_reciente, efectividad
            FROM motor_pesos_hora
            WHERE hora = :hora
            ORDER BY generacion DESC LIMIT 1
        """), {"hora": hora})).fetchone()

        rent = (await db.execute(text("""
            SELECT efectividad_top1, efectividad_top3, es_rentable, total_sorteos
            FROM rentabilidad_hora WHERE hora = :hora
        """), {"hora": hora})).fetchone()

        # Animal previo para señal Markov
        prev = (await db.execute(text("""
            SELECT animalito FROM historico
            WHERE loteria='Lotto Activo' AND hora=:hora
            ORDER BY fecha DESC OFFSET 1 LIMIT 1
        """), {"hora": hora})).fetchone()
        animal_previo = prev[0] if prev else None

        markov_top = []
        if animal_previo:
            mk = (await db.execute(text("""
                SELECT animal_sig, ROUND(probabilidad::numeric*100,2)
                FROM markov_transiciones
                WHERE hora=:hora AND LOWER(TRIM(animal_previo))=LOWER(TRIM(:animal))
                ORDER BY probabilidad DESC LIMIT 3
            """), {"hora": hora, "animal": animal_previo})).fetchall()
            markov_top = [{"animal": r[0], "prob_pct": float(r[1])} for r in mk]

        return {
            "status": "success",
            "hora": hora,
            "prediccion_1":     p[3] if p else None,
            "prediccion_2":     p[4] if p else None,
            "prediccion_3":     p[5] if p else None,
            "animal_predicho":  p[2] if p else None,
            "confianza_pct":    float(p[6]) if p and p[6] else 0,
            "confianza_hora":   float(p[7]) if p and p[7] else 0,
            "es_hora_rentable": bool(p[8]) if p and p[8] is not None else False,
            "ef_top1":   float(rent[0]) if rent else 0,
            "ef_top3":   float(rent[1]) if rent else 0,
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
# MARKOV — Top transiciones globales
# ═══════════════════════════════════════════════════════════
@app.get("/markov/top")
async def markov_top(limit: int = Query(default=20), db: AsyncSession = Depends(get_db)):
    try:
        rows = (await db.execute(text("""
            SELECT hora, animal_previo, animal_sig,
                   frecuencia,
                   ROUND(probabilidad::numeric * 100, 2) AS probabilidad_pct
            FROM markov_transiciones
            ORDER BY probabilidad DESC
            LIMIT :limit
        """), {"limit": limit})).fetchall()
        return [
            {
                "hora": r[0], "animal_previo": r[1], "animal_sig": r[2],
                "frecuencia": int(r[3]),
                "probabilidad_pct": float(r[4]) if r[4] else 0,
            }
            for r in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# MARKOV — Por animal+hora
# ═══════════════════════════════════════════════════════════
@app.get("/markov")
async def markov_animal(
    hora:   str = Query(...),
    animal: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    try:
        rows = (await db.execute(text("""
            SELECT hora, animal_previo, animal_sig,
                   frecuencia,
                   ROUND(probabilidad::numeric * 100, 2) AS probabilidad_pct
            FROM markov_transiciones
            WHERE hora = :hora
              AND LOWER(TRIM(animal_previo)) = LOWER(TRIM(:animal))
            ORDER BY probabilidad DESC
            LIMIT 20
        """), {"hora": hora, "animal": animal})).fetchall()
        return [
            {
                "hora": r[0], "animal_previo": r[1], "animal_sig": r[2],
                "frecuencia": int(r[3]),
                "probabilidad_pct": float(r[4]) if r[4] else 0,
            }
            for r in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# HISTORIAL — con pred1/2/3, offset y doble alias de filtros
# ═══════════════════════════════════════════════════════════
@app.get("/historial")
async def get_historial(
    fecha:     str = None,
    resultado: str = None,
    animal:    str = None,
    limit:     int = 200,
    offset:    int = 0,
    db: AsyncSession = Depends(get_db)
):
    try:
        conditions = [
            "h.animalito IS NOT NULL",
            "h.loteria='Lotto Activo'",
            "a.prediccion_1 IS NOT NULL",
        ]
        params = {"limit": limit, "offset": offset}

        if fecha:
            conditions.append("a.fecha=:fecha")
            params["fecha"] = fecha
        if animal:
            conditions.append("""(a.prediccion_1 ILIKE :animal OR a.prediccion_2 ILIKE :animal
                OR a.prediccion_3 ILIKE :animal OR h.animalito ILIKE :animal)""")
            params["animal"] = f"%{animal}%"
        if resultado in ("win", "true"):
            conditions.append("""h.animalito IN (
                COALESCE(a.prediccion_1,'__'),COALESCE(a.prediccion_2,'__'),
                COALESCE(a.prediccion_3,'__'))""")
        elif resultado in ("fail", "false"):
            conditions.append("""h.animalito NOT IN (
                COALESCE(a.prediccion_1,'__'),COALESCE(a.prediccion_2,'__'),
                COALESCE(a.prediccion_3,'__'))""")

        where = " AND ".join(conditions)
        rows = (await db.execute(text(f"""
            SELECT a.fecha, a.hora,
                   a.prediccion_1, a.prediccion_2, a.prediccion_3,
                   a.confianza_pct, h.animalito AS resultado,
                   a.es_hora_rentable, a.acierto,
                   CASE WHEN h.animalito IN (
                       COALESCE(a.prediccion_1,'__'),
                       COALESCE(a.prediccion_2,'__'),
                       COALESCE(a.prediccion_3,'__')
                   ) THEN true ELSE false END AS acierto_top3
            FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE {where}
            ORDER BY a.fecha DESC, a.hora DESC
            LIMIT :limit OFFSET :offset
        """), params)).fetchall()

        predicciones = [
            {
                "fecha":          r[0].strftime("%Y-%m-%d") if r[0] else "—",
                "hora":           str(r[1]) if r[1] else "—",
                "prediccion_1":   r[2] or "—",
                "prediccion_2":   r[3] or "—",
                "prediccion_3":   r[4] or "—",
                "pred1": r[2] or "—", "pred2": r[3] or "—", "pred3": r[4] or "—",
                "confianza_pct":  round(float(r[5]),1) if r[5] else None,
                "resultado_real": r[6] or "—",
                "resultado":      r[6] or "—",
                "hora_rentable":  bool(r[7]) if r[7] is not None else False,
                "acierto":        bool(r[8]) if r[8] is not None else bool(r[9]),
                "acierto_top3":   bool(r[9]),
            }
            for r in rows
        ]
        total = len(predicciones)
        ac1   = sum(1 for p in predicciones if p["acierto"] is True)
        ac3   = sum(1 for p in predicciones if p["acierto_top3"])
        return {
            "predicciones": predicciones, "registros": predicciones, "data": predicciones,
            "stats": {
                "total": total, "aciertos": ac3,
                "aciertos_top1": ac1, "aciertos_top3": ac3,
                "fallos": total - ac3,
                "efectividad": round(ac3/total*100,2) if total>0 else 0,
            },
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": str(e), "predicciones": [], "registros": [], "stats": {}
        })


# ═══════════════════════════════════════════════════════════
# RENTABILIDAD — V10 con ganancia estimada y aliases
# ═══════════════════════════════════════════════════════════
@app.get("/rentabilidad")
async def get_rentabilidad(db: AsyncSession = Depends(get_db)):
    try:
        res = (await db.execute(text("""
            SELECT hora, total_sorteos, aciertos_top1, aciertos_top3,
                   efectividad_top1, efectividad_top3,
                   es_rentable, ultima_actualizacion
            FROM rentabilidad_hora ORDER BY efectividad_top3 DESC
        """))).fetchall()
        horas = []
        for r in res:
            ef3 = float(r[5] or 0)
            horas.append({
                "hora":             r[0],
                "total_sorteos":    int(r[1] or 0),
                "aciertos_top1":    int(r[2] or 0),
                "aciertos_top3":    int(r[3] or 0),
                "efectividad_top1": float(r[4] or 0),
                "efectividad_top3": ef3,
                "es_rentable":      bool(r[6]),
                "ultima_actualizacion": str(r[7]) if r[7] else None,
                "vs_azar":    round(ef3 - 8.33, 2),
                "ventaja_pct": round(ef3 - 3.33, 2),
                "ganancia_x1": round(ef3/100*30 - (1 - ef3/100), 2),
                "señal": (
                    "✅ OPERAR" if bool(r[6])
                    else "⚠️ MARGINAL" if ef3 >= 8.0
                    else "❌ NO OPERAR"
                ),
            })
        rentables = [h for h in horas if h["es_rentable"]]
        return {
            "umbral_minimo": 3.33, "umbral_top3": 10.0,
            "pago_loteria": 30, "n_animales": 3, "azar_top3": 8.33,
            "horas_rentables": len(rentables),
            "mejor_hora": horas[0] if horas else None,
            "detalle": horas, "horas": horas, "data": horas,
            "resumen": (
                f"{len(rentables)}/{len(horas)} horas rentables. "
                f"Mejor: {horas[0]['hora']} ({horas[0]['efectividad_top3']}%)"
                if horas else "Sin datos"
            ),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# ENDPOINTS SIN CAMBIOS
# ═══════════════════════════════════════════════════════════
@app.get("/cargar-ultimo")
async def cargar_ultimo(db: AsyncSession = Depends(get_db)):
    try:
        from app.routes.cargarhist import capturar_ultimo_resultado
        resultado = await capturar_ultimo_resultado(db)
        return {"message": f"✅ Capturado: {resultado}", "ok": True}
    except ImportError:
        return {"message": "⚠️ capturar_ultimo_resultado no expuesta en cargarhist", "ok": False}
    except Exception as e:
        return {"message": f"❌ Error: {str(e)}", "ok": False}


@app.get("/procesar")
async def procesar(db: AsyncSession = Depends(get_db)):
    return await entrenar_modelo(db)


@app.get("/aprender")
async def aprender(background_tasks: BackgroundTasks, desde: str = None):
    """
    Lanza el aprendizaje en BACKGROUND — no bloquea el servidor.
    Llama /tarea para ver el progreso.
    """
    if _tarea["estado"] == "running":
        return {"status": "ya_corriendo", "tarea": _tarea["nombre"],
                "progreso": _tarea["progreso"]}
    from datetime import date
    fecha_inicio = date(2018, 1, 1)
    if desde:
        try: fecha_inicio = date.fromisoformat(desde)
        except: return {"error": "Formato inválido. Use YYYY-MM-DD"}
    background_tasks.add_task(_run_aprender, fecha_inicio)
    return {"status": "iniciado", "message": f"Aprendizaje desde {fecha_inicio} corriendo en background",
            "tip": "Llama /tarea para ver el progreso"}


@app.get("/pesos")
async def ver_pesos(db: AsyncSession = Depends(get_db)):
    try:
        rows = (await db.execute(text("""
            SELECT id,fecha,peso_reciente,peso_deuda,peso_anti,peso_patron,
                   peso_secuencia,efectividad,total_evaluados,aciertos,generacion
            FROM motor_pesos ORDER BY id DESC LIMIT 10
        """))).fetchall()
        rows_hora = (await db.execute(text("""
            SELECT hora,generacion,peso_decay,peso_markov,peso_gap,peso_reciente,efectividad
            FROM motor_pesos_hora ORDER BY hora, generacion DESC
        """))).fetchall()
        return {
            "historial_pesos": [
                {"generacion":r[10],"fecha":str(r[1]),
                 "pesos":{"reciente":r[2],"deuda":r[3],"anti":r[4],"patron":r[5],"secuencia":r[6]},
                 "efectividad":r[7],"total":r[8],"aciertos":r[9]}
                for r in rows
            ],
            "pesos_por_hora_v10": [
                {"hora":r[0],"generacion":r[1],
                 "pesos":{"decay":r[2],"markov":r[3],"gap":r[4],"reciente":r[5]},
                 "efectividad":r[6]}
                for r in rows_hora
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/retroactivo")
async def retroactivo(
    background_tasks: BackgroundTasks,
    desde: str = None, hasta: str = None, dias: int = 30,
):
    """
    Lanza el retroactivo en BACKGROUND — no bloquea el servidor.
    Llama /tarea para ver progreso.
    """
    if _tarea["estado"] == "running":
        return {"status": "ya_corriendo", "tarea": _tarea["nombre"],
                "progreso": _tarea["progreso"]}
    from datetime import date
    fd = date(2018, 1, 1)
    fh = date.today()
    if desde:
        try: fd = date.fromisoformat(desde)
        except: return {"error": "Formato 'desde' inválido"}
    if hasta:
        try: fh = date.fromisoformat(hasta)
        except: return {"error": "Formato 'hasta' inválido"}
    background_tasks.add_task(_run_retroactivo, fd, fh, dias)
    return {"status": "iniciado", "message": f"Retroactivo {fd}→{fh} corriendo en background",
            "tip": "Llama /tarea para ver el progreso"}


@app.get("/tarea")
async def ver_tarea():
    """Estado de la tarea larga en curso (aprender o retroactivo)."""
    t = dict(_tarea)
    # No devolver resultado completo si es muy grande
    if t.get("resultado") and isinstance(t["resultado"], dict):
        r = t["resultado"]
        t["resultado_resumen"] = {
            "status":            r.get("status"),
            "message":           r.get("message"),
            "efectividad_top1":  r.get("efectividad_top1"),
            "efectividad_top3":  r.get("efectividad_top3"),
            "procesados":        r.get("procesados") or r.get("total_sorteos_evaluados"),
        }
        t.pop("resultado")
    return t


@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    return {"stats": await obtener_estadisticas(db), "bitacora_hoy": await obtener_bitacora(db)}


@app.get("/backtest")
async def run_backtest(desde: str, hasta: str, db: AsyncSession = Depends(get_db)):
    from datetime import date
    try:
        fd = date.fromisoformat(desde)
        fh = date.fromisoformat(hasta)
        if (fh - fd).days > 180:
            return {"error": "Rango máximo: 6 meses"}
        return await backtest(db, fd, fh, max_sorteos=100)
    except ValueError:
        return {"error": "Formato inválido. Use YYYY-MM-DD"}




@app.get("/aprender-sql")
async def aprender_sql(db: AsyncSession = Depends(get_db)):
    """
    Entrenamiento masivo en SQL puro — sin tablas TEMP (incompatibles con Neon pool).
    Usa CTEs en memoria. Tiempo esperado: 5-20 segundos.
    """
    import time
    t0 = time.time()
    try:
        # PASO 1: Reconstruir probabilidades_hora
        await db.execute(text("DELETE FROM probabilidades_hora"))
        await db.execute(text("""
            INSERT INTO probabilidades_hora
                (hora, animalito, frecuencia, probabilidad, tendencia, ultima_actualizacion)
            WITH base AS (
                SELECT hora, animalito, COUNT(*) AS frec,
                    SUM(COUNT(*)) OVER (PARTITION BY hora) AS total_hora
                FROM historico WHERE loteria='Lotto Activo'
                GROUP BY hora, animalito
            ),
            rec AS (
                SELECT hora, animalito, COUNT(*) AS frec_rec
                FROM historico
                WHERE fecha >= CURRENT_DATE - INTERVAL '60 days' AND loteria='Lotto Activo'
                GROUP BY hora, animalito
            )
            SELECT b.hora, b.animalito, b.frec,
                ROUND((b.frec::FLOAT / NULLIF(b.total_hora,0) * 100)::numeric, 2),
                CASE WHEN COALESCE(r.frec_rec,0) >= 2 THEN 'CALIENTE' ELSE 'FRIO' END,
                NOW()
            FROM base b LEFT JOIN rec r ON b.hora=r.hora AND b.animalito=r.animalito
        """))
        await db.commit()

        # PASO 2: Upsert masivo en auditoria_ia usando CTE (sin tablas TEMP)
        r = await db.execute(text("""
            WITH top3_hora AS (
                SELECT hora,
                    MAX(CASE WHEN rk=1 THEN animalito END) AS pred1,
                    MAX(CASE WHEN rk=2 THEN animalito END) AS pred2,
                    MAX(CASE WHEN rk=3 THEN animalito END) AS pred3,
                    MAX(CASE WHEN rk=1 THEN probabilidad END) -
                    MAX(CASE WHEN rk=2 THEN probabilidad END) AS conf_diff
                FROM (
                    SELECT hora, animalito, probabilidad,
                        RANK() OVER (PARTITION BY hora ORDER BY probabilidad DESC) AS rk
                    FROM probabilidades_hora
                ) sub
                WHERE rk <= 3
                GROUP BY hora
            )
            INSERT INTO auditoria_ia
                (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                 confianza_pct, resultado_real, acierto)
            SELECT
                h.fecha, h.hora,
                t.pred1, t.pred1, t.pred2, t.pred3,
                LEAST(GREATEST(ROUND(COALESCE(t.conf_diff,0) * 100), 0), 100)::FLOAT,
                h.animalito,
                (LOWER(TRIM(t.pred1)) = LOWER(TRIM(h.animalito)))
            FROM historico h
            JOIN top3_hora t ON t.hora = h.hora
            WHERE h.loteria = 'Lotto Activo' AND t.pred1 IS NOT NULL
            ON CONFLICT (fecha, hora) DO UPDATE SET
                prediccion_1    = EXCLUDED.prediccion_1,
                prediccion_2    = EXCLUDED.prediccion_2,
                prediccion_3    = EXCLUDED.prediccion_3,
                animal_predicho = EXCLUDED.animal_predicho,
                confianza_pct   = EXCLUDED.confianza_pct,
                resultado_real  = EXCLUDED.resultado_real,
                acierto         = EXCLUDED.acierto
            WHERE auditoria_ia.prediccion_1 IS NULL
               OR auditoria_ia.resultado_real IS NULL
               OR auditoria_ia.resultado_real IN ('PENDIENTE', '')
        """))
        insertados = r.rowcount
        await db.commit()

        # PASO 3: Actualizar rentabilidad_hora
        await db.execute(text("""
            INSERT INTO rentabilidad_hora
                (hora, total_sorteos, aciertos_top1, aciertos_top3,
                 efectividad_top1, efectividad_top3, es_rentable, ultima_actualizacion)
            SELECT
                hora,
                COUNT(*) AS total,
                COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS ac1,
                COUNT(CASE WHEN
                    resultado_real IS NOT NULL
                    AND resultado_real NOT IN ('PENDIENTE','')
                    AND LOWER(TRIM(resultado_real)) IN (
                        LOWER(TRIM(COALESCE(prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_3,'__')))
                    ) THEN 1 END) AS ac3,
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::numeric /
                    NULLIF(COUNT(*),0)*100, 2) AS ef1,
                ROUND(COUNT(CASE WHEN
                    resultado_real IS NOT NULL
                    AND resultado_real NOT IN ('PENDIENTE','')
                    AND LOWER(TRIM(resultado_real)) IN (
                        LOWER(TRIM(COALESCE(prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_3,'__')))
                    ) THEN 1 END)::numeric /
                    NULLIF(COUNT(*),0)*100, 2) AS ef3,
                (ROUND(COUNT(CASE WHEN
                    resultado_real IS NOT NULL
                    AND resultado_real NOT IN ('PENDIENTE','')
                    AND LOWER(TRIM(resultado_real)) IN (
                        LOWER(TRIM(COALESCE(prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_3,'__')))
                    ) THEN 1 END)::numeric /
                    NULLIF(COUNT(*),0)*100, 2) >= 10.0) AS rentable,
                NOW()
            FROM auditoria_ia
            WHERE acierto IS NOT NULL
            GROUP BY hora
            ON CONFLICT (hora) DO UPDATE SET
                total_sorteos    = EXCLUDED.total_sorteos,
                aciertos_top1    = EXCLUDED.aciertos_top1,
                aciertos_top3    = EXCLUDED.aciertos_top3,
                efectividad_top1 = EXCLUDED.efectividad_top1,
                efectividad_top3 = EXCLUDED.efectividad_top3,
                es_rentable      = EXCLUDED.es_rentable,
                ultima_actualizacion = NOW()
        """))
        await db.commit()

        # PASO 4: Reconstruir markov_transiciones completo (pares consecutivos por hora)
        await db.execute(text("DELETE FROM markov_transiciones"))
        await db.execute(text("""
            INSERT INTO markov_transiciones (hora, animal_previo, animal_sig, frecuencia, probabilidad)
            WITH pares AS (
                SELECT
                    h1.hora,
                    h1.animalito AS animal_previo,
                    h2.animalito AS animal_sig
                FROM historico h1
                JOIN historico h2
                    ON  h2.fecha    = h1.fecha + INTERVAL '1 day'
                    AND h1.hora     = h2.hora
                    AND h1.loteria  = 'Lotto Activo'
                    AND h2.loteria  = 'Lotto Activo'
            ),
            conteos AS (
                SELECT hora, animal_previo, animal_sig,
                       COUNT(*) AS frec,
                       SUM(COUNT(*)) OVER (PARTITION BY hora, animal_previo) AS total_prev
                FROM pares
                GROUP BY hora, animal_previo, animal_sig
            )
            SELECT hora, animal_previo, animal_sig,
                   frec,
                   ROUND((frec::FLOAT / NULLIF(total_prev,0) * 100)::numeric, 2)
            FROM conteos
            ON CONFLICT (hora, animal_previo, animal_sig) DO UPDATE SET
                frecuencia   = EXCLUDED.frecuencia,
                probabilidad = EXCLUDED.probabilidad
        """))
        await db.commit()
        markov_n = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0

        # Métricas finales
        res = (await db.execute(text("""
            SELECT
                COUNT(*),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                COUNT(CASE WHEN
                    resultado_real IS NOT NULL
                    AND resultado_real NOT IN ('PENDIENTE','')
                    AND LOWER(TRIM(resultado_real)) IN (
                        LOWER(TRIM(COALESCE(prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(prediccion_3,'__')))
                    ) THEN 1 END)
            FROM auditoria_ia WHERE acierto IS NOT NULL
        """))).fetchone()

        total   = int(res[0] or 0)
        ac1     = int(res[1] or 0)
        ac3     = int(res[2] or 0)
        ef1     = round(ac1/total*100, 2) if total > 0 else 0
        ef3     = round(ac3/total*100, 2) if total > 0 else 0
        elapsed = round(time.time() - t0, 1)

        return {
            "status":           "success",
            "tiempo_seg":       elapsed,
            "insertados":       insertados,
            "total_calibrados": total,
            "efectividad_top1": ef1,
            "efectividad_top3": ef3,
            "markov_transiciones": int(markov_n),
            "message": (
                f"✅ Entrenado en {elapsed}s | "
                f"{insertados:,} filas | "
                f"Top1: {ef1}% | Top3: {ef3}% | "
                f"Markov: {int(markov_n):,} transiciones"
            )
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "LOTTOAI PRO V10", "markov": True, "decay": True}
