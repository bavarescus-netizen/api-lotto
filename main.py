import os, re, asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito
from app.services.motor_v5 import (
    generar_prediccion, obtener_estadisticas, obtener_bitacora,
    entrenar_modelo, backtest, calibrar_predicciones,
    llenar_auditoria_retroactiva, aprender_desde_historico,
    obtener_pesos_actuales,
)

app = FastAPI(title="LOTTOAI PRO V8")

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


# ─────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def iniciar_bot():
    async for db in get_db():
        try:
            await db.execute(text("""
                ALTER TABLE auditoria_ia
                ADD CONSTRAINT IF NOT EXISTS auditoria_fecha_hora_unique UNIQUE (fecha,hora)
            """))
            await db.commit()
        except Exception:
            await db.rollback()
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS motor_pesos (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    peso_reciente FLOAT DEFAULT 0.30,
                    peso_deuda FLOAT DEFAULT 0.25,
                    peso_anti FLOAT DEFAULT 0.25,
                    peso_patron FLOAT DEFAULT 0.10,
                    peso_secuencia FLOAT DEFAULT 0.10,
                    efectividad FLOAT DEFAULT 0.0,
                    total_evaluados INT DEFAULT 0,
                    aciertos INT DEFAULT 0,
                    generacion INT DEFAULT 1
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
        break
    asyncio.create_task(ciclo_infinito())
    print("🚀 LOTTOAI PRO V8 — Motor con aprendizaje activo")


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        stats_data = await obtener_estadisticas(db)

        # ── FIX: ahora incluye fecha formateada para mostrar en tarjetas ──
        res_db = await db.execute(text("""
            SELECT
                h.fecha,
                h.hora,
                h.animalito,
                a.acierto,
                a.animal_predicho
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            WHERE h.loteria = 'Lotto Activo'
            ORDER BY h.fecha DESC, h.hora DESC
            LIMIT 12
        """))

        ultimos_db = []
        for r in res_db.fetchall():
            nombre_animal  = re.sub(r'[^a-z]', '', r[2].lower())
            predicho_raw   = re.sub(r'[^a-z]', '', (r[4] or '').lower())
            # Formato MM-DD para mostrar en la tarjeta (corto y legible)
            fecha_str = r[0].strftime("%m-%d") if r[0] else "—"
            ultimos_db.append({
                "fecha":   fecha_str,        # ← NUEVO: visible en la tarjeta
                "hora":    r[1],
                "animal":  r[2],             # nombre completo para mostrar
                "img":     f"{nombre_animal}.png",
                "acierto": r[3],             # True / False / None
                "predicho": predicho_raw,
            })

        return templates.TemplateResponse("dashboard.html", {
            "request":          request,
            "top3":             res_ia.get("top3", []),
            "ultimos_db":       ultimos_db,
            "efectividad":      stats_data.get("efectividad_global", 0),
            "aciertos_hoy":     stats_data.get("aciertos_hoy", 0),
            "sorteos_hoy":      stats_data.get("sorteos_hoy", 0),
            "total_historico":  stats_data.get("total_historico", 0),
            "ultimo_resultado": res_ia.get("ultimo_resultado", "N/A"),
            "analisis":         res_ia.get("analisis", ""),
            "confianza_idx":    res_ia.get("confianza_idx", 0),
            "señal_texto":      res_ia.get("señal_texto", ""),
        })
    except Exception as e:
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/historial")
async def get_historial(
    fecha:     str = None,   # filtro: "2026-03-09"
    resultado: str = None,   # "win" | "fail"
    animal:    str = None,   # buscar por nombre
    limit:     int = 200,
    db: AsyncSession = Depends(get_db),
):
    """
    Pestaña Historial: predicciones con resultado real + acierto/fallo.
    El frontend (tab Historial) consume este endpoint via fetch('/historial').
    """
    try:
        # Construimos el WHERE dinámicamente
        conditions = ["h.animalito IS NOT NULL", "h.loteria = 'Lotto Activo'"]
        params: dict = {"limit": limit}

        if fecha:
            conditions.append("a.fecha = :fecha")
            params["fecha"] = fecha
        if animal:
            conditions.append("""(
                a.prediccion_1 ILIKE :animal OR
                a.prediccion_2 ILIKE :animal OR
                a.prediccion_3 ILIKE :animal OR
                h.animalito    ILIKE :animal
            )""")
            params["animal"] = f"%{animal}%"
        if resultado == "win":
            conditions.append("""
                h.animalito IN (
                    COALESCE(a.prediccion_1,''),
                    COALESCE(a.prediccion_2,''),
                    COALESCE(a.prediccion_3,'')
                )
            """)
        elif resultado == "fail":
            conditions.append("""
                h.animalito NOT IN (
                    COALESCE(a.prediccion_1,''),
                    COALESCE(a.prediccion_2,''),
                    COALESCE(a.prediccion_3,'')
                )
            """)

        where = " AND ".join(conditions)

        rows = await db.execute(text(f"""
            SELECT
                a.fecha,
                a.hora,
                a.prediccion_1,
                a.prediccion_2,
                a.prediccion_3,
                a.confianza_pct,
                h.animalito AS resultado,
                CASE
                    WHEN h.animalito IN (
                        COALESCE(a.prediccion_1,''),
                        COALESCE(a.prediccion_2,''),
                        COALESCE(a.prediccion_3,'')
                    ) THEN true
                    ELSE false
                END AS acierto
            FROM auditoria_ia a
            JOIN historico h
                ON h.fecha  = a.fecha
                AND h.hora   = a.hora
                AND h.loteria = 'Lotto Activo'
            WHERE {where}
            ORDER BY a.fecha DESC, a.hora DESC
            LIMIT :limit
        """), params)

        predicciones = []
        for r in rows.fetchall():
            predicciones.append({
                "fecha":     r[0].strftime("%Y-%m-%d") if r[0] else "—",
                "hora":      str(r[1]) if r[1] else "—",
                "pred1":     r[2] or "—",
                "pred2":     r[3] or "—",
                "pred3":     r[4] or "—",
                "confianza": round(float(r[5]), 1) if r[5] else None,
                "resultado": r[6] or "—",
                "acierto":   bool(r[7]),
            })

        total    = len(predicciones)
        aciertos = sum(1 for p in predicciones if p["acierto"])
        efectividad = round((aciertos / total * 100), 2) if total > 0 else 0.0

        return {
            "predicciones": predicciones,
            "stats": {
                "total":       total,
                "aciertos":    aciertos,
                "fallos":      total - aciertos,
                "efectividad": efectividad,
            },
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "predicciones": [], "stats": {}}
        )


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/cargar-ultimo")
async def cargar_ultimo(db: AsyncSession = Depends(get_db)):
    """
    Captura el último resultado publicado y lo guarda en la tabla historico.
    Reutiliza la lógica de cargarhist si está disponible, o hace el INSERT directo.
    """
    try:
        # Intenta usar el router de cargarhist si tiene una función callable
        from app.routes.cargarhist import capturar_ultimo_resultado
        resultado = await capturar_ultimo_resultado(db)
        return {"message": f"✅ Capturado: {resultado}", "ok": True}
    except ImportError:
        # Fallback: registra el último resultado conocido de la API/scraping
        return {"message": "⚠️ Módulo cargarhist no tiene capturar_ultimo_resultado expuesta", "ok": False}
    except Exception as e:
        return {"message": f"❌ Error: {str(e)}", "ok": False}


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/procesar")
async def procesar(db: AsyncSession = Depends(get_db)):
    return await entrenar_modelo(db)


@app.get("/aprender")
async def aprender(desde: str = None, db: AsyncSession = Depends(get_db)):
    """
    Aprendizaje por refuerzo — ajusta pesos solos basado en historia.
    Uso:
      /aprender                   → último año
      /aprender?desde=2020-01-01  → desde 2020 hasta hoy
      /aprender?desde=2018-01-01  → desde el inicio completo (~5 min)
    """
    from datetime import date
    fecha_inicio = None
    if desde:
        try: fecha_inicio = date.fromisoformat(desde)
        except: return {"error": "Formato inválido. Use YYYY-MM-DD"}
    return await aprender_desde_historico(db, fecha_inicio)


@app.get("/pesos")
async def ver_pesos(db: AsyncSession = Depends(get_db)):
    """Ver evolución histórica de los pesos aprendidos"""
    try:
        res = await db.execute(text("""
            SELECT id, fecha, peso_reciente, peso_deuda, peso_anti, peso_patron,
                   peso_secuencia, efectividad, total_evaluados, aciertos, generacion
            FROM motor_pesos ORDER BY id DESC LIMIT 10
        """))
        rows = res.fetchall()
        return {"historial_pesos": [{
            "generacion": r[10], "fecha": str(r[1]),
            "pesos": {
                "reciente":  r[2], "deuda":    r[3],
                "anti":      r[4], "patron":   r[5],
                "secuencia": r[6],
            },
            "efectividad": r[7], "total": r[8], "aciertos": r[9],
        } for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/retroactivo")
async def retroactivo(
    desde: str = None, hasta: str = None, dias: int = 30,
    db: AsyncSession = Depends(get_db),
):
    from datetime import date
    fecha_desde = fecha_hasta = None
    if desde:
        try: fecha_desde = date.fromisoformat(desde)
        except: return {"error": "Formato 'desde' inválido"}
    if hasta:
        try: fecha_hasta = date.fromisoformat(hasta)
        except: return {"error": "Formato 'hasta' inválido"}
    if fecha_desde and fecha_hasta and (fecha_hasta - fecha_desde).days > 366:
        return {"error": "Rango máximo 1 año"}
    return await llenar_auditoria_retroactiva(db, fecha_desde, fecha_hasta, dias)


@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    stats_data = await obtener_estadisticas(db)
    bitacora   = await obtener_bitacora(db)
    return {"stats": stats_data, "bitacora_hoy": bitacora}


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


@app.get("/estado")
async def estado_sistema(db: AsyncSession = Depends(get_db)):
    try:
        res_ultimo = await db.execute(text(
            "SELECT fecha, hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 1"))
        ultimo = res_ultimo.fetchone()

        res_pred = await db.execute(text(
            "SELECT fecha, hora, animal_predicho, confianza_pct, resultado_real, acierto "
            "FROM auditoria_ia ORDER BY fecha DESC, hora DESC LIMIT 1"))
        pred = res_pred.fetchone()

        res_met = await db.execute(text("""
            SELECT
                COUNT(*),
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),
                COUNT(CASE WHEN acierto = TRUE THEN 1 END),
                ROUND(
                    COUNT(CASE WHEN acierto = TRUE THEN 1 END)::numeric /
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END), 0) * 100, 1
                )
            FROM auditoria_ia
        """))
        met = res_met.fetchone()

        res_hoy = await db.execute(text(
            "SELECT hora, animal_predicho, resultado_real, acierto, confianza_pct "
            "FROM auditoria_ia WHERE fecha = CURRENT_DATE ORDER BY hora"))
        hoy = [
            {"hora": r[0], "predicho": r[1], "real": r[2],
             "acierto": r[3], "confianza": round(float(r[4] or 0))}
            for r in res_hoy.fetchall()
        ]

        res_hist = await db.execute(text(
            "SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM historico"))
        hist = res_hist.fetchone()

        import pytz
        from datetime import datetime
        tz     = pytz.timezone('America/Caracas')
        ahora  = datetime.now(tz)
        hora_actual = ahora.strftime("%I:00 %p").upper()

        res_deuda = await db.execute(text("""
            WITH ap AS (
                SELECT animalito, fecha,
                       LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
                FROM historico WHERE hora = :hora
            ),
            gaps AS (
                SELECT animalito, (fecha - fa) AS gap FROM ap WHERE fa IS NOT NULL
            ),
            ciclos AS (
                SELECT animalito, AVG(gap) AS ciclo FROM gaps
                GROUP BY animalito HAVING COUNT(*) >= 3
            ),
            ultima AS (
                SELECT animalito, CURRENT_DATE - MAX(fecha) AS dias
                FROM historico WHERE hora = :hora GROUP BY animalito
            )
            SELECT u.animalito, u.dias,
                   ROUND(c.ciclo::numeric, 1),
                   ROUND((u.dias / NULLIF(c.ciclo, 0) * 100)::numeric, 1)
            FROM ultima u JOIN ciclos c ON u.animalito = c.animalito
            ORDER BY 4 DESC LIMIT 5
        """), {"hora": hora_actual})
        top_deuda = [
            {"animal": r[0], "dias_ausente": int(r[1]),
             "ciclo_prom": float(r[2]), "deuda_pct": float(r[3])}
            for r in res_deuda.fetchall()
        ]

        pesos = await obtener_pesos_actuales(db)
        res_gen = await db.execute(text(
            "SELECT COALESCE(MAX(generacion), 1) FROM motor_pesos"))
        generacion = res_gen.scalar() or 1

        return {
            "estado": "✅ SISTEMA ACTIVO — Motor V8",
            "hora_venezolana": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "motor": {"version": "V8", "generacion": generacion, "pesos": pesos},
            "ultimo_capturado": {
                "fecha":  str(ultimo[0]) if ultimo else None,
                "hora":   ultimo[1]      if ultimo else None,
                "animal": ultimo[2]      if ultimo else None,
            },
            "ultima_prediccion": {
                "fecha":     str(pred[0]) if pred else None,
                "hora":      pred[1]      if pred else None,
                "predicho":  pred[2]      if pred else None,
                "confianza": round(float(pred[3] or 0)) if pred else 0,
                "real":      pred[4]      if pred else None,
                "acierto":   pred[5]      if pred else None,
            },
            "metricas": {
                "total_predicciones": int(met[0] or 0),
                "calibradas":         int(met[1] or 0),
                "aciertos":           int(met[2] or 0),
                "efectividad_pct":    float(met[3] or 0),
            },
            "historico": {
                "total_registros": int(hist[0] or 0),
                "desde": str(hist[1]) if hist[1] else None,
                "hasta": str(hist[2]) if hist[2] else None,
            },
            "predicciones_hoy":      hoy,
            "top_deuda_hora_actual": {"hora": hora_actual, "candidatos": top_deuda},
        }
    except Exception as e:
        return {"estado": f"❌ ERROR: {str(e)}"}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "LOTTOAI PRO V8"}
