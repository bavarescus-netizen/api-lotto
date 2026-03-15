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
        # PASO 1: limpiar tabla
        await db.execute(text("TRUNCATE TABLE markov_transiciones"))
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

        # PASO 3: verificar sanidad
        bad = (await db.execute(text(
            "SELECT COUNT(*) FROM markov_transiciones WHERE probabilidad > 100"
        ))).scalar() or 0

        return {
            "status": "success" if not errores else "partial",
            "transiciones": total,
            "prob_invalidas": bad,
            "errores": errores,
            "message": (
                f"✅ Markov: {total:,} transiciones | Inválidas: {bad}"
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
    Walk-forward backtest: entrena con datos ANTES de año_corte,
    evalúa en datos DESDE año_corte. Mide EF.TOP3 real por hora.
    Sin contaminar: el motor nunca ve los datos que va a predecir.
    """
    from datetime import date as _date
    from collections import Counter, defaultdict as _dd
    HORAS_BT = [
        "08:00 AM","09:00 AM","10:00 AM","11:00 AM",
        "12:00 PM","01:00 PM","02:00 PM","03:00 PM",
        "04:00 PM","05:00 PM","06:00 PM","07:00 PM",
    ]
    fecha_corte = _date(año_corte, 1, 1)
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

        # ── TEST: desde el corte ──
        test_rows = (await db.execute(text("""
            SELECT fecha, animalito FROM historico
            WHERE hora=:hora AND loteria='Lotto Activo' AND fecha >= :corte
            ORDER BY fecha ASC
        """), {"hora": hora, "corte": fecha_corte})).fetchall()

        if len(test_rows) < 30:
            resultados_hora[hora] = {"n_test": len(test_rows), "skip": True}
            continue

        animales_train = [r[0] for r in train_rows]
        n_train = len(animales_train)

        # ── Señales iniciales sobre TRAIN ──
        freq = Counter(animales_train)
        total_f = max(n_train, 1)
        markov_c = _dd(lambda: _dd(int))
        for i in range(1, len(animales_train)):
            markov_c[animales_train[i-1]][animales_train[i]] += 1
        ultima_vez = {a: i for i, a in enumerate(animales_train)}

        acum = list(animales_train)
        top1_ok = top3_ok = n_test = 0

        for idx_t, (fecha_t, real) in enumerate(test_rows):
            prev = acum[-1]
            # Scores: deuda + frecuencia + markov
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

            # Walk-forward: actualizar con el resultado real
            acum.append(real)
            freq[real] = freq.get(real, 0) + 1
            total_f += 1
            markov_c[prev][real] += 1
            ultima_vez[real] = n_train + idx_t

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



# ═══════════════════════════════════════════════════════════════
# SPRINT 1 — WALK-FORWARD BACKTEST
# Entrena con datos hasta fecha_corte, predice el resto
# Mide EF.TOP3 real por hora, por señal, por año
# ═══════════════════════════════════════════════════════════════
@app.get("/backtest")
async def backtest(
    fecha_corte: str = Query(default="2025-01-01"),
    db: AsyncSession = Depends(get_db)
):
    """
    Walk-forward backtest sobre datos históricos.
    fecha_corte: entrenar con datos ANTES de esta fecha, testear DESPUÉS.
    Default: entrenar 2018-2024, testear 2025-2026.
    """
    import pytz
    from datetime import datetime as _dt, date as _date
    try:
        corte = _date.fromisoformat(fecha_corte)
    except Exception:
        corte = _date(2025, 1, 1)

    HORAS_BT = [
        '08:00 AM','09:00 AM','10:00 AM','11:00 AM',
        '12:00 PM','01:00 PM','02:00 PM','03:00 PM',
        '04:00 PM','05:00 PM','06:00 PM','07:00 PM',
    ]

    try:
        # ── Cargar TRAIN: datos antes de la fecha corte ──
        train_rows = (await db.execute(text("""
            SELECT fecha, hora, animalito
            FROM historico
            WHERE loteria='Lotto Activo' AND fecha < :corte
            ORDER BY fecha ASC, hora ASC
        """), {"corte": corte})).fetchall()

        # ── Cargar TEST: datos desde la fecha corte ──
        test_rows = (await db.execute(text("""
            SELECT fecha, hora, animalito
            FROM historico
            WHERE loteria='Lotto Activo' AND fecha >= :corte
            ORDER BY fecha ASC, hora ASC
        """), {"corte": corte})).fetchall()

        if not test_rows:
            return {"status": "error", "message": "Sin datos de test para esa fecha"}

        # ── Construir modelo desde TRAIN ──
        # Frecuencia por hora
        freq_hora = {}         # {hora: {animal: count}}
        # Markov por hora
        markov_hora = {}       # {hora: {animal_prev: {animal_sig: count}}}
        # Gap (última aparición) por hora
        ultima_aparicion = {}  # {hora: {animal: fecha_index}}
        # Patrón día de semana por hora
        patron_dia = {}        # {hora: {dia: {animal: count}}}

        for i, (fecha, hora, animal) in enumerate(train_rows):
            if hora not in freq_hora:
                freq_hora[hora] = {}
                markov_hora[hora] = {}
                ultima_aparicion[hora] = {}
                patron_dia[hora] = {}
            # frecuencia
            freq_hora[hora][animal] = freq_hora[hora].get(animal, 0) + 1
            # patrón día
            dia = fecha.weekday()
            if dia not in patron_dia[hora]:
                patron_dia[hora][dia] = {}
            patron_dia[hora][dia][animal] = patron_dia[hora][dia].get(animal, 0) + 1
            # última aparición
            ultima_aparicion[hora][animal] = i

        # Markov requiere pares consecutivos por hora (mismo par fecha consecutiva)
        # Agrupar train por hora
        train_por_hora = {}
        for fecha, hora, animal in train_rows:
            if hora not in train_por_hora:
                train_por_hora[hora] = []
            train_por_hora[hora].append((fecha, animal))

        for hora, sorteos in train_por_hora.items():
            markov_hora[hora] = {}
            for i in range(1, len(sorteos)):
                f_prev, a_prev = sorteos[i-1]
                f_curr, a_curr = sorteos[i]
                # Solo pares de días consecutivos
                if (f_curr - f_prev).days == 1:
                    if a_prev not in markov_hora[hora]:
                        markov_hora[hora][a_prev] = {}
                    markov_hora[hora][a_prev][a_curr] = (
                        markov_hora[hora][a_prev].get(a_curr, 0) + 1
                    )

        # ── Función de predicción simplificada (top3) ──
        ANIMALES_ALL = [
            'carnero','toro','ciempies','alacran','pavo','cabra','burro','elefante',
            'camello','lechon','yegua','gallo','mono','paloma','oso','lechuza',
            'gato','caballo','perro','loro','pato','aguila','rana','cebra',
            'iguana','gallina','lapa','leon','jirafa','tortuga','delfin',
            'perico','ballena','caiman','tigre','venado','ardilla','cochino',
            'culebra','chivo','pescado','vaca','zamuro',
        ]

        def predecir_top3(hora, animal_previo, dia_semana, total_train_hora):
            scores = {}
            fh = freq_hora.get(hora, {})
            total = max(sum(fh.values()), 1)

            for a in ANIMALES_ALL:
                # Señal 1: frecuencia reciente (base)
                s_freq = fh.get(a, 0) / total

                # Señal 2: Markov
                s_markov = 0
                if animal_previo and hora in markov_hora:
                    mk = markov_hora[hora].get(animal_previo, {})
                    total_mk = max(sum(mk.values()), 1)
                    s_markov = mk.get(a, 0) / total_mk

                # Señal 3: patrón día de semana
                s_patron = 0
                if hora in patron_dia and dia_semana in patron_dia[hora]:
                    pd = patron_dia[hora][dia_semana]
                    total_pd = max(sum(pd.values()), 1)
                    s_patron = pd.get(a, 0) / total_pd

                # Señal 4: anti-repetición (si salió ayer, penalizar)
                penaliz = 0.85 if a == animal_previo else 1.0

                # Score combinado (pesos base — sprint 2 los optimizará)
                scores[a] = (
                    s_freq   * 0.35 +
                    s_markov * 0.30 +
                    s_patron * 0.20 +
                    s_freq   * 0.15  # deuda simplificada
                ) * penaliz

            top3 = sorted(scores, key=scores.get, reverse=True)[:3]
            return top3, scores

        # ── Evaluar TEST ──
        resultados_hora = {h: {"total":0,"top1":0,"top3":0,"top1_real":[],"top3_real":[]} for h in HORAS_BT}
        resultados_año  = {}
        resultados_mes  = {}

        # Para Markov en test: necesitamos saber el animal previo (último de esa hora antes)
        # Combinar train_por_hora con lo que va del test
        test_por_hora = {}
        for fecha, hora, animal in test_rows:
            if hora not in test_por_hora:
                test_por_hora[hora] = []
            test_por_hora[hora].append((fecha, animal))

        for hora in HORAS_BT:
            sorteos_train = train_por_hora.get(hora, [])
            sorteos_test  = test_por_hora.get(hora, [])
            if not sorteos_test:
                continue

            # Concatenar para obtener animal previo correcto
            todos = sorteos_train + sorteos_test
            idx_inicio_test = len(sorteos_train)

            for i in range(idx_inicio_test, len(todos)):
                fecha_t, real = todos[i]
                # Animal previo: último de día anterior
                animal_previo = None
                for j in range(i-1, max(i-5, -1), -1):
                    fj, aj = todos[j]
                    if (fecha_t - fj).days == 1:
                        animal_previo = aj
                        break

                dia_semana = fecha_t.weekday()
                top3, _ = predecir_top3(hora, animal_previo, dia_semana, len(sorteos_train))

                año = fecha_t.year
                mes = f"{fecha_t.year}-{fecha_t.month:02d}"

                if hora not in resultados_hora:
                    resultados_hora[hora] = {"total":0,"top1":0,"top3":0}
                resultados_hora[hora]["total"] += 1

                if año not in resultados_año:
                    resultados_año[año] = {"total":0,"top1":0,"top3":0}
                resultados_año[año]["total"] += 1

                if mes not in resultados_mes:
                    resultados_mes[mes] = {"total":0,"top1":0,"top3":0}
                resultados_mes[mes]["total"] += 1

                if top3 and real == top3[0]:
                    resultados_hora[hora]["top1"] += 1
                    resultados_año[año]["top1"] += 1
                    resultados_mes[mes]["top1"] += 1

                if top3 and real in top3:
                    resultados_hora[hora]["top3"] += 1
                    resultados_año[año]["top3"] += 1
                    resultados_mes[mes]["top3"] += 1

        # ── Calcular efectividades ──
        azar_top3 = round(3/38*100, 2)
        azar_top1 = round(1/38*100, 2)

        por_hora = {}
        for h, d in resultados_hora.items():
            t = max(d["total"], 1)
            ef3 = round(d["top3"]/t*100, 2)
            ef1 = round(d["top1"]/t*100, 2)
            por_hora[h] = {
                "total": d["total"],
                "ef_top1": ef1,
                "ef_top3": ef3,
                "vs_azar": round(ef3 - azar_top3, 2),
                "ratio": round(ef3 / azar_top3, 2) if azar_top3 > 0 else 0,
                "señal": "OPERAR" if ef3 >= 12 else "MARGINAL" if ef3 >= 10 else "NO",
            }

        por_año = {}
        for a, d in sorted(resultados_año.items()):
            t = max(d["total"], 1)
            por_año[str(a)] = {
                "total": d["total"],
                "ef_top3": round(d["top3"]/t*100, 2),
                "ef_top1": round(d["top1"]/t*100, 2),
            }

        por_mes = {}
        for m, d in sorted(resultados_mes.items()):
            t = max(d["total"], 1)
            por_mes[m] = {
                "total": d["total"],
                "ef_top3": round(d["top3"]/t*100, 2),
            }

        total_test = sum(d["total"] for d in resultados_hora.values())
        total_top3 = sum(d["top3"] for d in resultados_hora.values())
        ef_global  = round(total_top3 / max(total_test, 1) * 100, 2)

        # Ordenar horas por EF.TOP3 desc
        mejor_hora = sorted(por_hora.items(), key=lambda x: -x[1]["ef_top3"])

        return {
            "status": "success",
            "config": {
                "fecha_corte": str(corte),
                "train_registros": len(train_rows),
                "test_registros": len(test_rows),
            },
            "global": {
                "ef_top3": ef_global,
                "ef_top1": round(sum(d["top1"] for d in resultados_hora.values()) / max(total_test,1) * 100, 2),
                "total_predicciones": total_test,
                "azar_top3": azar_top3,
                "vs_azar": round(ef_global - azar_top3, 2),
                "ratio_azar": round(ef_global / azar_top3, 2),
            },
            "por_hora": dict(mejor_hora),
            "por_año": por_año,
            "por_mes": por_mes,
            "horas_operar": [h for h, d in mejor_hora if d["ef_top3"] >= 10],
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


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
    """
    Procesa rápido: calibra filas pendientes + genera predicción para hora actual.
    Para entrenamiento completo usa /aprender-sql.
    """
    try:
        import pytz
        from datetime import datetime
        ahora = datetime.now(pytz.timezone('America/Caracas'))

        # PASO 1: Calibrar solo las filas pendientes (rápido)
        r = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto        = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (a.acierto IS NULL
                   OR a.resultado_real IS NULL
                   OR a.resultado_real IN ('PENDIENTE',''))
        """))
        calibradas = r.rowcount
        await db.commit()

        # PASO 2: Insertar predicción para la hora actual/próxima si no existe
        horas_map = {8:'08:00 AM',9:'09:00 AM',10:'10:00 AM',11:'11:00 AM',
                     12:'12:00 PM',13:'01:00 PM',14:'02:00 PM',15:'03:00 PM',
                     16:'04:00 PM',17:'05:00 PM',18:'06:00 PM',19:'07:00 PM'}
        h_actual = ahora.hour
        # Siguiente hora de sorteo
        siguiente = None
        for h in sorted(horas_map.keys()):
            if h > h_actual:
                siguiente = horas_map[h]; break
        if not siguiente:
            siguiente = '08:00 AM'

        # Obtener top3 de probabilidades_hora para esa hora
        top3 = (await db.execute(text("""
            SELECT animalito, probabilidad
            FROM probabilidades_hora
            WHERE hora = :hora
            ORDER BY probabilidad DESC LIMIT 3
        """), {"hora": siguiente})).fetchall()

        pred_insertada = 0
        if top3 and len(top3) >= 1:
            p1 = top3[0][0]; p2 = top3[1][0] if len(top3)>1 else p1
            p3 = top3[2][0] if len(top3)>2 else p2
            conf = round(float(top3[0][1] - (top3[1][1] if len(top3)>1 else top3[0][1]))*100, 1) if len(top3)>1 else 5.0

            # Calcular ef_top3 de esa hora
            rent = (await db.execute(text("""
                SELECT efectividad_top3, es_rentable
                FROM rentabilidad_hora WHERE hora=:hora
            """), {"hora": siguiente})).fetchone()
            ef3 = float(rent[0]) if rent else 0
            es_rent = bool(rent[1]) if rent else False

            await db.execute(text("""
                INSERT INTO auditoria_ia
                    (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                     confianza_pct, resultado_real, acierto, confianza_hora, es_hora_rentable)
                VALUES (:fecha, :hora, :p1, :p1, :p2, :p3,
                        :conf, 'PENDIENTE', NULL, :ef3, :rent)
                ON CONFLICT (fecha, hora) DO UPDATE SET
                    prediccion_1    = EXCLUDED.prediccion_1,
                    prediccion_2    = EXCLUDED.prediccion_2,
                    prediccion_3    = EXCLUDED.prediccion_3,
                    animal_predicho = EXCLUDED.animal_predicho,
                    confianza_hora  = EXCLUDED.confianza_hora,
                    es_hora_rentable= EXCLUDED.es_hora_rentable
                WHERE auditoria_ia.resultado_real IN ('PENDIENTE','')
                   OR auditoria_ia.resultado_real IS NULL
            """), {
                "fecha": ahora.date(), "hora": siguiente,
                "p1": p1, "p2": p2, "p3": p3, "conf": conf,
                "ef3": ef3, "rent": es_rent,
            })
            pred_insertada = 1
            await db.commit()

        # PASO 3: insertar predicciones retroactivas para horas de HOY sin predicción
        hoy = ahora.date()
        horas_hoy = (await db.execute(text("""
            SELECT DISTINCT h.hora FROM historico h
            WHERE h.fecha = :hoy AND h.loteria = 'Lotto Activo'
              AND NOT EXISTS (
                  SELECT 1 FROM auditoria_ia a
                  WHERE a.fecha = h.fecha AND a.hora = h.hora
              )
        """), {"hoy": hoy})).fetchall()

        retro = 0
        for (hora_r,) in horas_hoy:
            t3 = (await db.execute(text("""
                SELECT animalito, probabilidad FROM probabilidades_hora
                WHERE hora=:hora ORDER BY probabilidad DESC LIMIT 3
            """), {"hora": hora_r})).fetchall()
            if not t3: continue
            r2 = (await db.execute(text("""
                SELECT efectividad_top3, es_rentable FROM rentabilidad_hora WHERE hora=:hora
            """), {"hora": hora_r})).fetchone()
            rr = (await db.execute(text("""
                SELECT animalito FROM historico
                WHERE fecha=:hoy AND hora=:hora AND loteria='Lotto Activo' LIMIT 1
            """), {"hoy": hoy, "hora": hora_r})).fetchone()
            await db.execute(text("""
                INSERT INTO auditoria_ia
                    (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                     confianza_pct, resultado_real, acierto, confianza_hora, es_hora_rentable)
                VALUES (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :res, :ac, :ef3, :rent)
                ON CONFLICT (fecha, hora) DO NOTHING
            """), {
                "fecha": hoy, "hora": hora_r,
                "p1": t3[0][0], "p2": t3[1][0] if len(t3)>1 else t3[0][0],
                "p3": t3[2][0] if len(t3)>2 else t3[0][0],
                "conf": round(float(t3[0][1])*100, 1),
                "res": rr[0] if rr else "PENDIENTE",
                "ac": (t3[0][0].lower()==rr[0].lower()) if rr else None,
                "ef3": float(r2[0]) if r2 else 0,
                "rent": bool(r2[1]) if r2 else False,
            })
            retro += 1
        if retro: await db.commit()

        return {
            "status": "success",
            "calibradas": calibradas,
            "retro_insertadas": retro,
            "prediccion_hora": siguiente,
            "pred_insertada": pred_insertada,
            "message": (f"✅ {calibradas} calibradas | {retro} retro | "
                       f"Pred → {siguiente}: {top3[0][0].upper() if top3 else '?'}")
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


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
