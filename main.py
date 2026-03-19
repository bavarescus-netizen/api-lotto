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
    migrar_schema, actualizar_resultados_señales, obtener_score_señales,
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

        # ── Predicción para el PRÓXIMO sorteo ──
        # Calcular hora del próximo sorteo (Venezuela UTC-4)
        _mn = ahora.minute
        _h  = ahora.hour
        _slots = [8,9,10,11,12,13,14,15,16,17,18,19]
        _lbls  = {8:'08:00 AM',9:'09:00 AM',10:'10:00 AM',11:'11:00 AM',
                  12:'12:00 PM',13:'01:00 PM',14:'02:00 PM',15:'03:00 PM',
                  16:'04:00 PM',17:'05:00 PM',18:'06:00 PM',19:'07:00 PM'}
        if _h < 8:
            _hora_prox = _lbls[8]
        elif _h >= 19:
            _hora_prox = _lbls[8]
        elif _mn > 2:
            _sig = _h + 1
            _hora_prox = _lbls.get(_sig, _lbls[8])
        else:
            _hora_prox = _lbls.get(_h, _lbls[8])

        # Buscar predicción de hoy para esa hora
        p = (await db.execute(text(
            "SELECT fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto,"
            "prediccion_1,prediccion_2,prediccion_3,"
            "COALESCE(confianza_hora,0),COALESCE(es_hora_rentable,FALSE) "
            "FROM auditoria_ia "
            "WHERE fecha=:hoy AND hora=:hora "
            "ORDER BY fecha DESC LIMIT 1"
        ), {"hoy": ahora.date(), "hora": _hora_prox})).fetchone()

        # Si no hay predicción guardada para la próxima hora → generarla en vivo
        if not p:
            try:
                from app.core.motor_v10 import generar_prediccion
                _pred_live = await generar_prediccion(db)
                if _pred_live and _pred_live.get("prediccion_1"):
                    # Construir tupla compatible
                    p = (
                        ahora.date(),
                        _pred_live.get("hora", _hora_prox),
                        _pred_live.get("prediccion_1"),
                        _pred_live.get("confianza_pct", 0),
                        None, None,
                        _pred_live.get("prediccion_1"),
                        _pred_live.get("prediccion_2"),
                        _pred_live.get("prediccion_3"),
                        _pred_live.get("confianza_hora", 0),
                        _pred_live.get("es_hora_rentable", False),
                    )
            except Exception:
                # Fallback: última predicción de la BD
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

        # Normalizar hora: "10:00 AM", "10:00AM", "10:00 am" → formato estándar
        try:
            from datetime import datetime as _dt
            _h = hora.strip().upper().replace("  ", " ")
            # Asegurar formato "HH:MM AM/PM"
            if " " not in _h:
                _h = _h[:-2] + " " + _h[-2:]
            hora = _dt.strptime(_h, "%I:%M %p").strftime("%I:%M %p")
        except Exception:
            pass

        # rent PRIMERO — pw lo necesita
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

        # Si no hay pesos diferenciados, calcular adaptativos
        if not pw or (pw[0]==pw[1]==pw[2]==pw[3]):
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
            pw = (round(p_decay,2), round(p_markov,2), round(p_gap,2), p_rec, ef3)

        # Animal previo Markov (penúltimo sorteo de esa hora)
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
@app.get("/historial")
async def get_historial(
    limit: int = Query(default=50),
    offset: int = Query(default=0),
    fecha: str = Query(default=None),
    resultado: str = Query(default=None),
    animal: str = Query(default=None),
    db: AsyncSession = Depends(get_db)
):
    """Historial paginado de predicciones con filtros."""
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
            "registros": [
                {
                    "fecha":         str(r[0]),
                    "hora":          r[1],
                    "prediccion_1":  r[2] or "",
                    "prediccion_2":  r[3] or "",
                    "prediccion_3":  r[4] or "",
                    "resultado_real": r[5] or "PENDIENTE",
                    "acierto":       r[6],
                    "confianza_pct": int(r[7] or 0),
                }
                for r in rows
            ],
            "total": len(rows),
            "offset": offset,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


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
                   END AS probabilidad_pct
            FROM markov_transiciones
            ORDER BY probabilidad_pct DESC
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


@app.get("/markov")
async def markov_buscar(
    hora: str = Query(default="08:00 AM"),
    animal: str = Query(default=""),
    db: AsyncSession = Depends(get_db)
):
    """Busca transiciones Markov para un animal+hora específicos."""
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
            WHERE hora = :hora
              AND LOWER(animal_previo) = :animal
            ORDER BY frecuencia DESC
            LIMIT 15
        """), {"hora": hora, "animal": animal_norm})).fetchall()
        return [
            {
                "animal_previo": r[0], "animal_sig": r[1],
                "frecuencia": int(r[2]), "probabilidad_pct": float(r[3]) if r[3] else 0,
            }
            for r in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════
# MARKOV — Por animal+hora
# ═══════════════════════════════════════════════════════════
@app.get("/diagnostico-markov")
async def diagnostico_markov(db: AsyncSession = Depends(get_db)):
    """
    Diagnóstico completo de markov_transiciones.
    Muestra: estructura real, constraints, conteos, muestra de datos corruptos.
    """
    resultado = {}
    try:
        # 1. Estructura de la tabla (columnas y tipos)
        r = await db.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'markov_transiciones'
            ORDER BY ordinal_position
        """))
        resultado["columnas"] = [
            {"col": row[0], "tipo": row[1], "nullable": row[2]}
            for row in r.fetchall()
        ]

        # 2. Constraints (PRIMARY KEY, UNIQUE, etc.)
        r = await db.execute(text("""
            SELECT conname, contype, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'markov_transiciones'::regclass
        """))
        resultado["constraints"] = [
            {"nombre": row[0], "tipo": row[1], "def": row[2]}
            for row in r.fetchall()
        ]

        # 3. Conteos generales
        r = await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))
        resultado["total_filas"] = r.scalar() or 0

        r = await db.execute(text("SELECT COUNT(*) FROM markov_transiciones WHERE probabilidad > 100"))
        resultado["filas_prob_mayor_100"] = r.scalar() or 0

        r = await db.execute(text("SELECT COUNT(*) FROM markov_transiciones WHERE probabilidad <= 0"))
        resultado["filas_prob_cero_o_neg"] = r.scalar() or 0

        r = await db.execute(text("SELECT MAX(probabilidad), MIN(probabilidad), AVG(probabilidad) FROM markov_transiciones"))
        row = r.fetchone()
        resultado["prob_max"]  = float(row[0]) if row[0] else None
        resultado["prob_min"]  = float(row[1]) if row[1] else None
        resultado["prob_avg"]  = round(float(row[2]), 2) if row[2] else None

        # 4. Muestra de las 5 filas más corruptas
        r = await db.execute(text("""
            SELECT hora, animal_previo, animal_sig, frecuencia, probabilidad
            FROM markov_transiciones
            ORDER BY probabilidad DESC
            LIMIT 10
        """))
        resultado["top10_por_prob"] = [
            {"hora": row[0], "previo": row[1], "sig": row[2],
             "frec": row[3], "prob": float(row[4])}
            for row in r.fetchall()
        ]

        # 5. Filas por hora
        r = await db.execute(text("""
            SELECT hora, COUNT(*), MAX(probabilidad), AVG(probabilidad)
            FROM markov_transiciones
            GROUP BY hora ORDER BY hora
        """))
        resultado["por_hora"] = [
            {"hora": row[0], "filas": row[1],
             "prob_max": round(float(row[2]),2) if row[2] else None,
             "prob_avg": round(float(row[3]),2) if row[3] else None}
            for row in r.fetchall()
        ]

        # 6. Verificar si la tabla viene de un schema diferente
        r = await db.execute(text("""
            SELECT schemaname, tablename
            FROM pg_tables WHERE tablename = 'markov_transiciones'
        """))
        resultado["schema"] = [{"schema": row[0], "tabla": row[1]} for row in r.fetchall()]

        resultado["diagnostico"] = (
            "CORRUPTO — probabilidades > 100%" if resultado["filas_prob_mayor_100"] > 0
            else "OK — probabilidades válidas" if resultado["total_filas"] > 1000
            else "VACIO — tabla sin datos suficientes"
        )

    except Exception as e:
        resultado["error"] = str(e)

    return resultado


@app.get("/fix-markov-directo")
async def fix_markov_directo(db: AsyncSession = Depends(get_db)):
    """
    Normaliza probabilidades corruptas DIRECTAMENTE con UPDATE.
    No borra ni reconstruye — solo divide las probs > 100 para dejarlas en rango correcto.
    Más rápido y seguro que el fix completo.
    """
    try:
        # Ver cuántas están corruptas antes
        antes = (await db.execute(text(
            "SELECT COUNT(*), MAX(probabilidad) FROM markov_transiciones WHERE probabilidad > 100"
        ))).fetchone()
        n_corruptas = int(antes[0] or 0)
        prob_max_antes = float(antes[1] or 0)

        if n_corruptas == 0:
            total = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0
            return {
                "status": "ok",
                "mensaje": f"✅ Ya estaba limpio. {total:,} transiciones, todas válidas.",
                "corruptas_antes": 0,
                "prob_max": prob_max_antes,
            }

        # Recalcular probabilidades correctas directamente
        await db.execute(text("""
            UPDATE markov_transiciones m
            SET probabilidad = ROUND(
                (m.frecuencia::FLOAT /
                 NULLIF(sub.total_prev, 0) * 100)::numeric, 2
            )
            FROM (
                SELECT hora, animal_previo,
                       SUM(frecuencia) AS total_prev
                FROM markov_transiciones
                GROUP BY hora, animal_previo
            ) sub
            WHERE m.hora = sub.hora
              AND m.animal_previo = sub.animal_previo
        """))
        await db.commit()

        # Verificar resultado
        despues = (await db.execute(text(
            "SELECT COUNT(*), MAX(probabilidad), AVG(probabilidad) FROM markov_transiciones WHERE probabilidad > 100"
        ))).fetchone()
        n_restantes = int(despues[0] or 0)

        # Si aún quedan, eliminarlos
        if n_restantes > 0:
            await db.execute(text("DELETE FROM markov_transiciones WHERE probabilidad > 100"))
            await db.commit()

        total = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0
        prob_max = (await db.execute(text("SELECT MAX(probabilidad) FROM markov_transiciones"))).scalar() or 0

        return {
            "status": "success",
            "corruptas_antes": n_corruptas,
            "prob_max_antes": prob_max_antes,
            "corruptas_restantes": n_restantes,
            "prob_max_ahora": float(prob_max),
            "total_transiciones": total,
            "mensaje": f"✅ Corregidas {n_corruptas} filas. Prob máx ahora: {float(prob_max):.2f}%",
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "mensaje": str(e)}



@app.get("/fix-markov")
async def fix_markov(db: AsyncSession = Depends(get_db)):
    """Reconstruye markov_transiciones hora por hora para evitar timeout de Render"""
    HORAS = [
        '08:00 AM','09:00 AM','10:00 AM','11:00 AM',
        '12:00 PM','01:00 PM','02:00 PM','03:00 PM',
        '04:00 PM','05:00 PM','06:00 PM','07:00 PM',
    ]
    SQL_HORA = """
        INSERT INTO markov_transiciones
            (hora, animal_previo, animal_sig, frecuencia, probabilidad)
        WITH pares AS (
            SELECT h1.hora,
                   h1.animalito AS animal_previo,
                   h2.animalito AS animal_sig
            FROM historico h1
            JOIN historico h2
                ON  h2.fecha   = h1.fecha + INTERVAL '1 day'
                AND h1.hora    = h2.hora
                AND h1.loteria = 'Lotto Activo'
                AND h2.loteria = 'Lotto Activo'
                AND h1.hora    = :hora
        ),
        conteos AS (
            SELECT hora, animal_previo, animal_sig,
                   COUNT(*)                                              AS frec,
                   SUM(COUNT(*)) OVER (PARTITION BY hora, animal_previo) AS total_prev
            FROM pares
            GROUP BY hora, animal_previo, animal_sig
        )
        SELECT hora, animal_previo, animal_sig,
               frec,
               ROUND((frec::FLOAT / NULLIF(total_prev, 0) * 100)::numeric, 2)
        FROM conteos
        ON CONFLICT (hora, animal_previo, animal_sig) DO UPDATE SET
            frecuencia   = EXCLUDED.frecuencia,
            probabilidad = EXCLUDED.probabilidad
    """
    try:
        # PASO 1: limpiar tabla con DELETE (más compatible con transacciones que TRUNCATE)
        await db.execute(text("DELETE FROM markov_transiciones"))
        await db.commit()

        # Verificar que quedó vacía
        n_antes = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0
        if n_antes > 0:
            # Forzar con TRUNCATE si DELETE no alcanzó
            await db.execute(text("TRUNCATE TABLE markov_transiciones RESTART IDENTITY"))
            await db.commit()

        # PASO 2: INSERT por hora (12 queries pequeños, cada uno < 3s)
        total = 0
        errores = []
        for hora in HORAS:
            try:
                await db.execute(text(SQL_HORA), {"hora": hora})
                await db.commit()
                n_hora = (await db.execute(text(
                    "SELECT COUNT(*) FROM markov_transiciones WHERE hora=:h"
                ), {"h": hora})).scalar() or 0
                total += n_hora
            except Exception as e_hora:
                await db.rollback()
                errores.append(f"{hora}: {e_hora}")

        # PASO 3: verificar sanidad — prob > 100 es corrupto
        bad = (await db.execute(text(
            "SELECT COUNT(*) FROM markov_transiciones WHERE probabilidad > 100"
        ))).scalar() or 0

        # Si todavía hay corruptos, eliminarlos
        if bad > 0:
            await db.execute(text("DELETE FROM markov_transiciones WHERE probabilidad > 100"))
            await db.commit()

        return {
            "status": "success" if not errores else "partial",
            "transiciones": total,
            "prob_invalidas": bad,
            "errores": errores,
            "message": (
                f"✅ Markov: {total:,} transiciones | Inválidas eliminadas: {bad}"
                + (f" | Errores: {errores}" if errores else "")
            )
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}



@app.get("/backtest")
async def backtest_motor(
    año_corte: int = Query(default=2025, description="Año donde empieza el test set"),
    hora_filtro: str = Query(default="todas"),
    db: AsyncSession = Depends(get_db)
):
    """
    Walk-forward backtest por hora individual.
    Evalúa SOLO el año indicado (año_corte) — train = todo lo anterior.
    """
    from datetime import date as _date
    from collections import Counter, defaultdict as _dd
    HORAS_BT = [
        "08:00 AM","09:00 AM","10:00 AM","11:00 AM",
        "12:00 PM","01:00 PM","02:00 PM","03:00 PM",
        "04:00 PM","05:00 PM","06:00 PM","07:00 PM",
    ]
    fecha_corte = _date(año_corte, 1, 1)
    # Limitar test a UN solo año para que no haga timeout en Render
    fecha_fin   = _date(año_corte + 1, 1, 1)
    t0 = datetime.datetime.now()
    horas_a_evaluar = [hora_filtro] if hora_filtro != "todas" else HORAS_BT
    resultados_hora = {}
    total_test = total_top3 = total_top1 = 0

    for hora in horas_a_evaluar:
        # ── TRAIN: todo antes del corte ──
        train_rows = (await db.execute(text("""
            SELECT animalito FROM historico
            WHERE hora=:hora AND loteria='Lotto Activo' AND fecha < :corte
            ORDER BY fecha ASC
        """), {"hora": hora, "corte": fecha_corte})).fetchall()

        # ── TEST: solo el año seleccionado (no todos los años siguientes) ──
        test_rows = (await db.execute(text("""
            SELECT fecha, animalito FROM historico
            WHERE hora=:hora AND loteria='Lotto Activo'
              AND fecha >= :corte AND fecha < :fin
            ORDER BY fecha ASC
        """), {"hora": hora, "corte": fecha_corte, "fin": fecha_fin})).fetchall()

        if len(test_rows) < 10:
            resultados_hora[hora] = {"n_test": len(test_rows), "skip": True}
            continue

        animales_train = [r[0] for r in train_rows]
        n_train = len(animales_train)

        # ── TEST: usar predicciones ya guardadas en auditoria_ia ──
        # Esto refleja el motor V10 REAL con todas sus señales
        # Walk-forward real: cada predicción fue hecha con datos hasta ese día
        res_audit = (await db.execute(text("""
            SELECT
                a.fecha,
                LOWER(TRIM(a.prediccion_1)) AS pred1,
                LOWER(TRIM(a.prediccion_2)) AS pred2,
                LOWER(TRIM(a.prediccion_3)) AS pred3,
                LOWER(TRIM(h.animalito))    AS real,
                a.confianza_pct
            FROM auditoria_ia a
            JOIN historico h
                ON h.fecha = a.fecha
                AND h.hora = a.hora
                AND h.loteria = 'Lotto Activo'
            WHERE a.hora = :hora
              AND a.fecha >= :corte
              AND a.fecha <  :fin
              AND a.prediccion_1 IS NOT NULL
              AND h.animalito IS NOT NULL
            ORDER BY a.fecha ASC
        """), {"hora": hora, "corte": fecha_corte, "fin": fecha_fin})).fetchall()

        if len(res_audit) < 10:
            # Fallback: motor simplificado si no hay datos en auditoria_ia
            acum = list(animales_train)
            top1_ok = top3_ok = n_test = 0
            freq = Counter(animales_train)
            total_f = max(n_train, 1)
            markov_c = _dd(lambda: _dd(int))
            for i in range(1, len(animales_train)):
                markov_c[animales_train[i-1]][animales_train[i]] += 1
            ultima_vez = {a: i for i, a in enumerate(animales_train)}

            for idx_t, (fecha_t, real) in enumerate(test_rows):
                prev = acum[-1]
                all_a = set(list(freq.keys()) + [real])
                scores = {}
                for a in all_a:
                    gap = (n_train + idx_t) - ultima_vez.get(a, 0)
                    intervalo_esp = total_f / max(freq.get(a, 1), 1)
                    deuda_s = min(gap / max(intervalo_esp, 1), 3.0) / 3.0
                    freq_s  = freq.get(a, 0.5 / total_f) / total_f
                    mk_total = sum(markov_c[prev].values())
                    mk_s    = markov_c[prev].get(a, 0) / max(mk_total, 1)
                    scores[a] = deuda_s*0.28 + freq_s*0.25 + mk_s*0.25 + (1/38)*0.22
                top3 = sorted(scores, key=scores.get, reverse=True)[:3]
                if top3[0] == real: top1_ok += 1
                if real in top3:    top3_ok += 1
                n_test += 1
                acum.append(real)
                freq[real] = freq.get(real, 0) + 1
                total_f += 1
                markov_c[prev][real] += 1
                ultima_vez[real] = n_train + idx_t
        else:
            # Motor V10 REAL — predicciones reales guardadas
            top1_ok = top3_ok = n_test = 0
            for row in res_audit:
                fecha_t, pred1, pred2, pred3, real, conf = row
                if not real or not pred1:
                    continue
                top3 = [x for x in [pred1, pred2, pred3] if x]
                if top3 and top3[0] == real: top1_ok += 1
                if real in top3:             top3_ok += 1
                n_test += 1

        ef_t3 = round(top3_ok / n_test * 100, 2) if n_test else 0
        ef_t1 = round(top1_ok / n_test * 100, 2) if n_test else 0
        azar  = round(3/38*100, 2)
        resultados_hora[hora] = {
            "n_train": n_train, "n_test": n_test,
            "top1_ok": top1_ok, "top3_ok": top3_ok,
            "ef_top1": ef_t1,   "ef_top3": ef_t3,
            "vs_azar": round(ef_t3 - azar, 2),
            "ratio":   round(ef_t3 / azar, 2),
            "rentable": ef_t3 >= 10.0,
        }
        total_test += n_test; total_top3 += top3_ok; total_top1 += top1_ok

    azar = round(3/38*100, 2)
    ef_g3 = round(total_top3 / max(total_test,1) * 100, 2)
    ef_g1 = round(total_top1 / max(total_test,1) * 100, 2)
    ranking = sorted(
        [(h,v) for h,v in resultados_hora.items() if not v.get("skip")],
        key=lambda x: x[1]["ef_top3"], reverse=True
    )
    return {
        "status":    "success",
        "año_corte": año_corte,
        "global": {
            "total_test": total_test,
            "ef_top1": ef_g1, "ef_top3": ef_g3,
            "vs_azar": round(ef_g3 - azar, 2),
            "ratio":   round(ef_g3 / azar, 2),
        },
        "ranking":   [{"hora": h, **v} for h, v in ranking],
        "por_hora":  resultados_hora,
        "azar_top3": azar,
        "tiempo_s":  round((datetime.datetime.now() - t0).total_seconds(), 1),
    }


@app.get("/optimizar-pesos")
async def optimizar_pesos_hora(
    hora: str = Query(default="08:00 AM"),
    año_corte: int = Query(default=2025),
    db: AsyncSession = Depends(get_db)
):
    """
    Grid search: prueba 125 combinaciones de pesos para UNA hora.
    Devuelve los 5 mejores combos con su EF.TOP3 real (walk-forward).
    Guarda el mejor en motor_pesos_hora.
    """
    from datetime import date as _date
    from collections import Counter, defaultdict as _dd
    import itertools

    fecha_corte = _date(año_corte, 1, 1)
    t0 = datetime.datetime.now()

    train_rows = (await db.execute(text("""
        SELECT animalito FROM historico
        WHERE hora=:hora AND loteria='Lotto Activo' AND fecha < :corte
        ORDER BY fecha ASC
    """), {"hora": hora, "corte": fecha_corte})).fetchall()

    test_rows = (await db.execute(text("""
        SELECT animalito FROM historico
        WHERE hora=:hora AND loteria='Lotto Activo' AND fecha >= :corte
        ORDER BY fecha ASC
    """), {"hora": hora, "corte": fecha_corte})).fetchall()

    if len(test_rows) < 50:
        return {"status": "error", "message": f"Insuficiente test data para {hora} ({len(test_rows)} filas)"}

    animales_train = [r[0] for r in train_rows]
    test_animales  = [r[0] for r in test_rows]

    # Pre-calcular señales base (se recalculan igual para todos los pesos)
    freq = Counter(animales_train)
    total_f = len(animales_train)
    markov_c = _dd(lambda: _dd(int))
    for i in range(1, len(animales_train)):
        markov_c[animales_train[i-1]][animales_train[i]] += 1
    ultima_vez = {a: i for i, a in enumerate(animales_train)}

    # Grid de pesos a probar (suman ~1.0)
    VALS = [0.10, 0.20, 0.30, 0.40, 0.50]
    mejores = []

    def evaluar_pesos(w_deuda, w_freq, w_mk, w_patron):
        suma = w_deuda + w_freq + w_mk + w_patron
        acum = list(animales_train)
        f = Counter(animales_train)
        tf = total_f
        mc = _dd(lambda: _dd(int))
        for i in range(1, len(animales_train)):
            mc[animales_train[i-1]][animales_train[i]] += 1
        uv = dict(ultima_vez)
        top3_ok = 0
        for idx_t, real in enumerate(test_animales):
            prev = acum[-1]
            all_a = set(list(f.keys()) + [real])
            scores = {}
            for a in all_a:
                gap = (total_f + idx_t) - uv.get(a, 0)
                iv  = tf / max(f.get(a,1),1)
                d_s = min(gap/max(iv,1), 3.0)/3.0
                f_s = f.get(a,0.5/tf)/tf
                mt  = sum(mc[prev].values())
                m_s = mc[prev].get(a,0)/max(mt,1)
                scores[a] = (d_s*w_deuda + f_s*w_freq + m_s*w_mk + (1/38)*w_patron)/suma
            top3 = sorted(scores, key=scores.get, reverse=True)[:3]
            if real in top3: top3_ok += 1
            acum.append(real); f[real]=f.get(real,0)+1; tf+=1
            mc[prev][real]+=1; uv[real]=total_f+idx_t
        return round(top3_ok/len(test_animales)*100, 2)

    # Reducir el grid a 125 combos relevantes
    combos_testados = 0
    for w1, w2, w3 in itertools.product(VALS, VALS, VALS):
        w4 = round(1.0 - w1 - w2 - w3, 2)
        if w4 < 0.05 or w4 > 0.60: continue
        ef = evaluar_pesos(w1, w2, w3, w4)
        mejores.append({"w_deuda":w1,"w_freq":w2,"w_mk":w3,"w_patron":w4,"ef_top3":ef})
        combos_testados += 1
        if combos_testados >= 100: break  # límite por tiempo

    mejores.sort(key=lambda x: -x["ef_top3"])
    top5 = mejores[:5]

    # Guardar el mejor en motor_pesos_hora
    if top5:
        mejor = top5[0]
        try:
            await db.execute(text("""
                INSERT INTO motor_pesos_hora
                    (hora, peso_decay, peso_markov, peso_gap, peso_reciente, efectividad, generacion)
                VALUES (:hora, :pd, :pm, :pg, :pr, :ef, NOW())
                ON CONFLICT (hora) DO UPDATE SET
                    peso_decay    = EXCLUDED.peso_decay,
                    peso_markov   = EXCLUDED.peso_markov,
                    peso_gap      = EXCLUDED.peso_gap,
                    peso_reciente = EXCLUDED.peso_reciente,
                    efectividad   = EXCLUDED.efectividad,
                    generacion    = NOW()
            """), {
                "hora": hora,
                "pd": mejor["w_deuda"],  "pm": mejor["w_mk"],
                "pg": mejor["w_freq"],   "pr": mejor["w_patron"],
                "ef": mejor["ef_top3"],
            })
            await db.commit()
        except Exception as e_p:
            await db.rollback()

    return {
        "status": "success",
        "hora": hora,
        "combos_testados": combos_testados,
        "azar_top3": round(3/38*100, 2),
        "top5_pesos": top5,
        "mejor_ef_top3": top5[0]["ef_top3"] if top5 else 0,
        "tiempo_s": round((datetime.datetime.now() - t0).total_seconds(), 1),
    }


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


@app.get("/diagnostico-animales")
async def diagnostico_animales(db: AsyncSession = Depends(get_db)):
    """
    Compara animales en la BD (historico) vs catálogo del motor.
    Detecta inconsistencias que causan predicciones perdidas.
    """
    # Catálogo oficial del motor
    MOTOR = {
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
    animales_motor = set(MOTOR.values())

    try:
        # Animales únicos en historico
        res = await db.execute(text("""
            SELECT LOWER(TRIM(animalito)) as animal, COUNT(*) as veces
            FROM historico
            WHERE loteria='Lotto Activo'
            GROUP BY LOWER(TRIM(animalito))
            ORDER BY veces DESC
        """))
        rows = res.fetchall()
        animales_bd = {r[0]: int(r[1]) for r in rows}

        # Comparar
        en_bd_no_motor = {a: v for a, v in animales_bd.items() if a not in animales_motor}
        en_motor_no_bd = {a for a in animales_motor if a not in animales_bd}

        # Animales en auditoria_ia
        res2 = await db.execute(text("""
            SELECT LOWER(TRIM(resultado_real)) as animal, COUNT(*) as veces
            FROM auditoria_ia
            WHERE resultado_real IS NOT NULL
              AND resultado_real != 'PENDIENTE'
            GROUP BY LOWER(TRIM(resultado_real))
            ORDER BY veces DESC
        """))
        rows2 = res2.fetchall()
        animales_auditoria = {r[0]: int(r[1]) for r in rows2}
        en_auditoria_no_motor = {a: v for a, v in animales_auditoria.items()
                                  if a not in animales_motor}

        return {
            "motor_total": len(animales_motor),
            "bd_total": len(animales_bd),
            "animales_motor": sorted(animales_motor),
            "animales_bd": animales_bd,
            "BUG_en_bd_no_en_motor": en_bd_no_motor,
            "en_motor_no_en_bd": sorted(en_motor_no_bd),
            "BUG_en_auditoria_no_en_motor": en_auditoria_no_motor,
            "diagnostico": (
                "✅ Catálogos coinciden" if not en_bd_no_motor and not en_motor_no_bd
                else f"❌ {len(en_bd_no_motor)} animales en BD no reconocidos por motor | "
                     f"{len(en_motor_no_bd)} en motor no encontrados en BD"
            )
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/auto-pesos")
async def auto_pesos(db: AsyncSession = Depends(get_db)):
    """
    Calcula pesos óptimos desde auditoria_señales y los guarda en motor_pesos.
    Basado en efectividad real de cada señal cuando fue dominante.
    Requiere mínimo 500 registros en auditoria_señales para ser confiable.
    """
    try:
        # Verificar muestra disponible
        n = (await db.execute(text(
            "SELECT COUNT(*) FROM auditoria_señales WHERE acierto_top3 IS NOT NULL"
        ))).scalar() or 0

        if n < 200:
            return {
                "status": "insuficiente",
                "registros": n,
                "message": f"Solo {n} registros con resultado. Necesita mínimo 200. Corre CARGAR 2018→HOY primero."
            }

        # Calcular efectividad por señal dominante
        res = await db.execute(text("""
            SELECT
                CASE
                    WHEN score_deuda >= GREATEST(score_reciente, score_patron_dia,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'deuda'
                    WHEN score_reciente >= GREATEST(score_deuda, score_patron_dia,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'reciente'
                    WHEN score_patron_dia >= GREATEST(score_deuda, score_reciente,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'patron'
                    WHEN score_markov >= GREATEST(score_deuda, score_reciente,
                         score_patron_dia, score_anti_racha, score_ciclo_exacto, score_patron_fecha)
                    THEN 'secuencia'
                    WHEN score_anti_racha >= GREATEST(score_deuda, score_reciente,
                         score_patron_dia, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'anti'
                    ELSE 'reciente'
                END AS señal,
                COUNT(*) AS total,
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END) AS aciertos
            FROM auditoria_señales
            WHERE acierto_top3 IS NOT NULL
            GROUP BY señal
        """))
        rows = res.fetchall()

        # Calcular efectividad por señal
        ef_señal = {}
        total_general = 0
        for r in rows:
            señal = r[0]
            total = int(r[1])
            ac = int(r[2])
            ef = ac / total if total > 0 else 0
            ef_señal[señal] = {"ef": ef, "total": total, "aciertos": ac}
            total_general += total

        azar = 3 / 38  # 7.89%

        # Convertir efectividad a pesos proporcionales
        # Señal que rinde más que azar → más peso, menos → menos peso
        señales_motor = ["reciente", "deuda", "anti", "patron", "secuencia"]
        pesos_raw = {}
        for s in señales_motor:
            if s in ef_señal:
                # Peso proporcional a qué tan por encima del azar está
                ratio = ef_señal[s]["ef"] / azar
                pesos_raw[s] = max(ratio, 0.5)  # mínimo 0.5× para no eliminar señales
            else:
                pesos_raw[s] = 1.0  # sin datos → peso neutro

        # Normalizar para que sumen ~1.0
        total_raw = sum(pesos_raw.values())
        pesos_norm = {s: round(v / total_raw, 4) for s, v in pesos_raw.items()}

        # Calcular EF global actual
        total_ac = sum(d["aciertos"] for d in ef_señal.values())
        total_tot = sum(d["total"] for d in ef_señal.values())
        ef_global = round(total_ac / total_tot * 100, 2) if total_tot > 0 else 0

        # Guardar en motor_pesos
        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),0) FROM motor_pesos"))
        gen = (res_gen.scalar() or 0) + 1

        await db.execute(text("""
            INSERT INTO motor_pesos
                (peso_reciente, peso_deuda, peso_anti, peso_patron, peso_secuencia,
                 efectividad, total_evaluados, aciertos, generacion)
            VALUES (:r, :d, :a, :p, :s, :ef, :tot, :ac, :gen)
        """), {
            "r":   pesos_norm["reciente"],
            "d":   pesos_norm["deuda"],
            "a":   pesos_norm["anti"],
            "p":   pesos_norm["patron"],
            "s":   pesos_norm["secuencia"],
            "ef":  ef_global,
            "tot": total_tot,
            "ac":  total_ac,
            "gen": gen,
        })

        # Actualizar también motor_pesos_hora con los mismos pesos base
        for hora in ["08:00 AM","09:00 AM","10:00 AM","11:00 AM",
                     "12:00 PM","01:00 PM","02:00 PM","03:00 PM",
                     "04:00 PM","05:00 PM","06:00 PM","07:00 PM"]:
            try:
                await db.execute(text("""
                    INSERT INTO motor_pesos_hora
                        (hora, generacion, peso_decay, peso_markov, peso_gap, peso_reciente,
                         efectividad, total_evaluados, aciertos_top3)
                    VALUES (:hora, :gen, :anti, :markov, :deuda, :rec, :ef, :tot, :ac)
                    ON CONFLICT (hora, generacion) DO UPDATE SET
                        peso_decay=EXCLUDED.peso_decay,
                        peso_markov=EXCLUDED.peso_markov,
                        peso_gap=EXCLUDED.peso_gap,
                        peso_reciente=EXCLUDED.peso_reciente,
                        efectividad=EXCLUDED.efectividad
                """), {
                    "hora": hora, "gen": gen,
                    "anti":   pesos_norm["anti"],
                    "markov": pesos_norm["secuencia"],
                    "deuda":  pesos_norm["deuda"],
                    "rec":    pesos_norm["reciente"],
                    "ef": ef_global, "tot": total_tot, "ac": total_ac,
                })
            except Exception:
                pass

        await db.commit()

        return {
            "status": "success",
            "registros_analizados": n,
            "ef_top3_global": ef_global,
            "pesos_anteriores": {"reciente": 0.25, "deuda": 0.28, "anti": 0.22, "patron": 0.15, "secuencia": 0.10},
            "pesos_nuevos": pesos_norm,
            "detalle_señales": {
                s: {
                    "ef_pct": round(ef_señal[s]["ef"] * 100, 1) if s in ef_señal else None,
                    "total": ef_señal[s]["total"] if s in ef_señal else 0,
                    "vs_azar": round(ef_señal[s]["ef"] / azar, 2) if s in ef_señal else None,
                }
                for s in señales_motor
            },
            "generacion": gen,
            "message": f"✅ Pesos actualizados en generación {gen} | EF.TOP3: {ef_global}% | {n:,} predicciones analizadas",
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}



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

        # PASO 4: Reconstruir markov_transiciones HORA POR HORA (evita timeout Render)
        _HORAS_MK = [
            '08:00 AM','09:00 AM','10:00 AM','11:00 AM',
            '12:00 PM','01:00 PM','02:00 PM','03:00 PM',
            '04:00 PM','05:00 PM','06:00 PM','07:00 PM',
        ]
        _SQL_MK_HORA = """
            INSERT INTO markov_transiciones
                (hora, animal_previo, animal_sig, frecuencia, probabilidad)
            WITH pares AS (
                SELECT h1.hora, h1.animalito AS animal_previo, h2.animalito AS animal_sig
                FROM historico h1
                JOIN historico h2
                    ON  h2.fecha   = h1.fecha + INTERVAL '1 day'
                    AND h1.hora    = h2.hora
                    AND h1.hora    = :hora
                    AND h1.loteria = 'Lotto Activo'
                    AND h2.loteria = 'Lotto Activo'
            ),
            conteos AS (
                SELECT hora, animal_previo, animal_sig,
                       COUNT(*) AS frec,
                       SUM(COUNT(*)) OVER (PARTITION BY hora, animal_previo) AS total_prev
                FROM pares GROUP BY hora, animal_previo, animal_sig
            )
            SELECT hora, animal_previo, animal_sig,
                   frec,
                   ROUND((frec::FLOAT / NULLIF(total_prev,0) * 100)::numeric, 2)
            FROM conteos
            ON CONFLICT DO NOTHING
        """
        try:
            await db.execute(text("TRUNCATE TABLE markov_transiciones"))
            await db.commit()
            for _h in _HORAS_MK:
                try:
                    await db.execute(text(_SQL_MK_HORA), {"hora": _h})
                    await db.commit()
                except Exception as _e_h:
                    await db.rollback()
                    logger.warning(f"PASO 4 markov hora {_h}: {_e_h}")
        except Exception as e_mk:
            await db.rollback()
            logger.error(f"PASO 4 markov TRUNCATE ERROR: {e_mk}")
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
async def health(db: AsyncSession = Depends(get_db)):
    """Keep-alive para cron-job.org — llámalo cada 5 min"""
    import pytz
    from datetime import datetime
    try:
        ahora = datetime.now(pytz.timezone('America/Caracas'))
        mk = (await db.execute(text("SELECT COUNT(*) FROM markov_transiciones"))).scalar() or 0

        # Recovery automática: si hay sorteos sin predecir de hoy → calibrar + predecir
        recovery_msg = None
        try:
            pendientes = (await db.execute(text("""
                SELECT COUNT(*) FROM auditoria_ia
                WHERE fecha = :hoy AND acierto IS NULL AND resultado_real IS NOT NULL
            """), {"hoy": ahora.date()})).scalar() or 0

            sin_prediccion = (await db.execute(text("""
                SELECT COUNT(*) FROM historico h
                WHERE h.fecha = :hoy
                  AND h.loteria = 'Lotto Activo'
                  AND NOT EXISTS (
                    SELECT 1 FROM auditoria_ia a
                    WHERE a.fecha = h.fecha AND a.hora = h.hora
                  )
            """), {"hoy": ahora.date()})).scalar() or 0

            if pendientes > 0 or sin_prediccion > 0:
                from app.core.motor_v10 import calibrar_predicciones, generar_prediccion
                await calibrar_predicciones(db)
                await generar_prediccion(db)
                recovery_msg = f"⚡ Recovery: {pendientes} calibradas, {sin_prediccion} predicciones insertadas"
        except Exception as re:
            recovery_msg = f"recovery_skip: {str(re)[:60]}"

        return {
            "status": "ok",
            "version": "LOTTOAI PRO V10",
            "hora_vzla": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "markov_transiciones": int(mk),
            "awake": True,
            "recovery": recovery_msg,
        }
    except Exception:
        return {"status": "ok", "awake": True}


# ══════════════════════════════════════════════════════
# SCORE POR SEÑAL — qué señal aporta valor real
# ══════════════════════════════════════════════════════
@app.get("/score-señales")
async def endpoint_score_señales(dias: int = 90, db: AsyncSession = Depends(get_db)):
    """
    Analiza auditoria_señales y muestra qué señal es realmente útil.
    Parámetro: dias=90 (por defecto últimos 90 días)
    """
    return await obtener_score_señales(db, dias=dias)


@app.post("/actualizar-señales")
async def endpoint_actualizar_señales(db: AsyncSession = Depends(get_db)):
    """
    Sincroniza resultado_real y aciertos en auditoria_señales.
    Llamar después de /entrenar o /calibrar.
    """
    resultado = await actualizar_resultados_señales(db)
    return {"status": "ok", **resultado}


@app.post("/migrar-señales")
async def endpoint_migrar_señales(db: AsyncSession = Depends(get_db)):
    """
    Crea la tabla auditoria_señales si no existe.
    Ejecutar una sola vez tras el deploy.
    """
    sqls = [
        """
        CREATE TABLE IF NOT EXISTS auditoria_señales (
            id              SERIAL PRIMARY KEY,
            fecha           DATE        NOT NULL,
            hora            VARCHAR(20) NOT NULL,
            animal_predicho VARCHAR(50),
            resultado_real  VARCHAR(50),
            acierto_top1    BOOLEAN,
            acierto_top3    BOOLEAN,
            confianza       INT DEFAULT 0,
            score_deuda         FLOAT DEFAULT 0,
            score_reciente      FLOAT DEFAULT 0,
            score_patron_dia    FLOAT DEFAULT 0,
            score_anti_racha    FLOAT DEFAULT 0,
            score_markov        FLOAT DEFAULT 0,
            score_ciclo_exacto  FLOAT DEFAULT 0,
            score_patron_fecha  FLOAT DEFAULT 0,
            score_final         FLOAT DEFAULT 0,
            peso_deuda      FLOAT DEFAULT 0,
            peso_reciente   FLOAT DEFAULT 0,
            peso_patron     FLOAT DEFAULT 0,
            peso_anti       FLOAT DEFAULT 0,
            peso_markov     FLOAT DEFAULT 0,
            creado_en       TIMESTAMP DEFAULT NOW(),
            UNIQUE (fecha, hora)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_audsig_fecha    ON auditoria_señales(fecha)",
        "CREATE INDEX IF NOT EXISTS idx_audsig_hora     ON auditoria_señales(hora)",
        "CREATE INDEX IF NOT EXISTS idx_audsig_acierto  ON auditoria_señales(acierto_top3)",
    ]
    errores = []
    for sql in sqls:
        try:
            await db.execute(text(sql))
        except Exception as e:
            errores.append(str(e)[:80])
    try:
        await db.commit()
    except Exception:
        await db.rollback()

    return {
        "status": "ok" if not errores else "parcial",
        "tabla": "auditoria_señales",
        "mensaje": "✅ Tabla lista para recibir desgloses de señales",
        "errores": errores,
    }


@app.post("/retroactivo")
async def endpoint_retroactivo_bloque(
    desde: str = "2018-01-01",
    hasta: str = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Procesa UN bloque de máximo 30 días y retorna.
    El dashboard llama esto en loop hasta cubrir 2018→hoy.
    Así nunca supera el timeout de Render gratuito (~25s por bloque).
    """
    from datetime import date as _date, timedelta
    try:
        fecha_desde = _date.fromisoformat(desde)
    except Exception:
        fecha_desde = _date(2018, 1, 1)

    if hasta:
        try:
            fecha_hasta = _date.fromisoformat(hasta)
        except Exception:
            fecha_hasta = fecha_desde + timedelta(days=30)
    else:
        fecha_hasta = fecha_desde + timedelta(days=30)

    # Nunca pasar de ayer
    tope = _date.today() - timedelta(days=1)
    if fecha_hasta > tope:
        fecha_hasta = tope

    return await llenar_auditoria_retroactiva(
        db, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta
    )


# ══════════════════════════════════════════════════════
# RENTABILIDAD POR HORA — Tab "Rentabilidad Horas"
# Lee de rentabilidad_hora + calcula en vivo desde auditoria_ia
# ══════════════════════════════════════════════════════
@app.get("/rentabilidad")
async def get_rentabilidad(db: AsyncSession = Depends(get_db)):
    try:
        # Leer tabla rentabilidad_hora (actualizada por /entrenar)
        rows = (await db.execute(text("""
            SELECT hora, total_sorteos, aciertos_top1, aciertos_top3,
                   efectividad_top1, efectividad_top3, es_rentable
            FROM rentabilidad_hora
            ORDER BY efectividad_top3 DESC
        """))).fetchall()

        if rows:
            return [
                {
                    "hora":            r[0],
                    "total":           int(r[1] or 0),
                    "aciertos_top1":   int(r[2] or 0),
                    "aciertos_top3":   int(r[3] or 0),
                    "ef_top1":         round(float(r[4] or 0), 2),
                    "ef_top3":         round(float(r[5] or 0), 2),
                    "es_rentable":     bool(r[6]),
                    "vs_azar":         round(float(r[5] or 0) - 7.89, 2),
                }
                for r in rows
            ]

        # Fallback: calcular en vivo desde auditoria_ia si la tabla está vacía
        rows2 = (await db.execute(text("""
            SELECT a.hora,
                COUNT(*) AS total,
                COUNT(CASE WHEN a.acierto=TRUE THEN 1 END) AS ac1,
                COUNT(CASE WHEN
                    LOWER(TRIM(h.animalito)) IN (
                        LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                    ) THEN 1 END) AS ac3
            FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
                AND h.loteria='Lotto Activo'
            WHERE a.acierto IS NOT NULL
            GROUP BY a.hora
            ORDER BY ac3::float/NULLIF(COUNT(*),0) DESC
        """))).fetchall()

        return [
            {
                "hora":          r[0],
                "total":         int(r[1]),
                "aciertos_top1": int(r[2]),
                "aciertos_top3": int(r[3]),
                "ef_top1":       round(int(r[2])/int(r[1])*100, 2) if r[1] else 0,
                "ef_top3":       round(int(r[3])/int(r[1])*100, 2) if r[1] else 0,
                "es_rentable":   (int(r[3])/int(r[1])*100 >= 10.0) if r[1] else False,
                "vs_azar":       round(int(r[3])/int(r[1])*100 - 7.89, 2) if r[1] else -7.89,
            }
            for r in rows2
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
