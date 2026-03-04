import os
import re
import asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito

from app.services.motor_v5 import (
    generar_prediccion,
    obtener_estadisticas,
    obtener_bitacora,
    entrenar_modelo,
    backtest,
    calibrar_predicciones,
    llenar_auditoria_retroactiva,
)

app = FastAPI(title="LOTTOAI PRO")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(prediccion.router)
app.include_router(cargarhist.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))


@app.on_event("startup")
async def iniciar_bot():
    async for db in get_db():
        try:
            await db.execute(text("""
                ALTER TABLE auditoria_ia
                ADD CONSTRAINT IF NOT EXISTS auditoria_fecha_hora_unique
                UNIQUE (fecha, hora)
            """))
            await db.commit()
        except Exception:
            await db.rollback()
        break
    asyncio.create_task(ciclo_infinito())
    print("🚀 LOTTOAI PRO iniciado — Bot de vigilancia activo")


# ══════════════════════════════════════════════
# HOME
# ══════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        stats_data = await obtener_estadisticas(db)
        res_db = await db.execute(text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto, a.animal_predicho
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha=a.fecha AND h.hora=a.hora
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """))
        ultimos_db = []
        for r in res_db.fetchall():
            nombre_animal = re.sub(r'[^a-z]','',r[2].lower())
            predicho_raw = re.sub(r'[^a-z]','',(r[4] or '').lower())
            ultimos_db.append({
                "hora": r[1], "animal": r[2],
                "img": f"{nombre_animal}.png",
                "acierto": r[3], "predicho": predicho_raw
            })
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3",[]),
            "ultimos_db": ultimos_db,
            "efectividad": stats_data.get("efectividad_global",0),
            "aciertos_hoy": stats_data.get("aciertos_hoy",0),
            "sorteos_hoy": stats_data.get("sorteos_hoy",0),
            "total_historico": stats_data.get("total_historico",0),
            "ultimo_resultado": res_ia.get("ultimo_resultado","N/A"),
            "analisis": res_ia.get("analisis",""),
            "confianza_idx": res_ia.get("confianza_idx",0),
            "señal_texto": res_ia.get("señal_texto",""),
        })
    except Exception as e:
        print(f"Error en Home: {e}")
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)


# ══════════════════════════════════════════════
# PROCESAR — Entrenar
# ══════════════════════════════════════════════
@app.get("/procesar")
async def procesar(db: AsyncSession = Depends(get_db)):
    return await entrenar_modelo(db)


# ══════════════════════════════════════════════
# RETROACTIVO — Llena auditoría con predicciones
# retroactivas de los últimos N días.
# Ejemplo: /retroactivo?dias=30
# Esto resuelve la efectividad 0% mostrando datos reales.
# ⚠️ Puede tardar 2-5 min. Llamar una sola vez.
# ══════════════════════════════════════════════
@app.get("/retroactivo")
async def retroactivo(dias: int = 30, db: AsyncSession = Depends(get_db)):
    if dias > 60:
        return {"error": "Máximo 60 días para evitar timeout"}
    return await llenar_auditoria_retroactiva(db, dias)


# ══════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════
@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    stats_data = await obtener_estadisticas(db)
    bitacora = await obtener_bitacora(db)
    return {"stats": stats_data, "bitacora_hoy": bitacora}


# ══════════════════════════════════════════════
# BACKTEST — máx 100 sorteos para evitar timeout
# Ejemplo: /backtest?desde=2025-01-01&hasta=2025-03-31
# ══════════════════════════════════════════════
@app.get("/backtest")
async def run_backtest(desde: str, hasta: str, db: AsyncSession = Depends(get_db)):
    from datetime import date
    try:
        fecha_desde = date.fromisoformat(desde)
        fecha_hasta = date.fromisoformat(hasta)
        if (fecha_hasta - fecha_desde).days > 180:
            return {"error": "Rango máximo: 6 meses"}
        return await backtest(db, fecha_desde, fecha_hasta, max_sorteos=100)
    except ValueError:
        return {"error": "Formato inválido. Use YYYY-MM-DD"}


# ══════════════════════════════════════════════
# ESTADO
# ══════════════════════════════════════════════
@app.get("/estado")
async def estado_sistema(db: AsyncSession = Depends(get_db)):
    try:
        res_ultimo = await db.execute(text(
            "SELECT fecha, hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 1"))
        ultimo = res_ultimo.fetchone()

        res_pred = await db.execute(text(
            "SELECT fecha, hora, animal_predicho, confianza_pct, resultado_real, acierto FROM auditoria_ia ORDER BY fecha DESC, hora DESC LIMIT 1"))
        pred = res_pred.fetchone()

        res_met = await db.execute(text("""
            SELECT COUNT(*),
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::numeric/
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100,1)
            FROM auditoria_ia
        """))
        met = res_met.fetchone()

        res_hoy = await db.execute(text(
            "SELECT hora, animal_predicho, resultado_real, acierto, confianza_pct FROM auditoria_ia WHERE fecha=CURRENT_DATE ORDER BY hora"))
        hoy = [{"hora":r[0],"predicho":r[1],"real":r[2],
                "acierto":r[3],"confianza":round(float(r[4] or 0))}
               for r in res_hoy.fetchall()]

        res_hist = await db.execute(text("SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM historico"))
        hist = res_hist.fetchone()

        import pytz
        from datetime import datetime
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_actual = ahora.strftime("%I:00 %p").upper()

        res_deuda = await db.execute(text("""
            WITH apariciones AS (
                SELECT animalito, fecha,
                    LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fecha_anterior
                FROM historico WHERE hora=:hora
            ),
            gaps AS (
                SELECT animalito, (fecha-fecha_anterior) AS gap_dias
                FROM apariciones WHERE fecha_anterior IS NOT NULL
            ),
            ciclos AS (
                SELECT animalito, AVG(gap_dias) AS ciclo_prom
                FROM gaps GROUP BY animalito HAVING COUNT(*)>=3
            ),
            ultima AS (
                SELECT animalito, CURRENT_DATE-MAX(fecha) AS dias_ausente
                FROM historico WHERE hora=:hora GROUP BY animalito
            )
            SELECT u.animalito, u.dias_ausente,
                ROUND(c.ciclo_prom::numeric,1),
                ROUND((u.dias_ausente/NULLIF(c.ciclo_prom,0)*100)::numeric,1)
            FROM ultima u JOIN ciclos c ON u.animalito=c.animalito
            ORDER BY 4 DESC LIMIT 5
        """), {"hora": hora_actual})
        top_deuda = [{"animal":r[0],"dias_ausente":int(r[1]),
                      "ciclo_prom":float(r[2]),"deuda_pct":float(r[3])}
                     for r in res_deuda.fetchall()]

        return {
            "estado": "✅ SISTEMA ACTIVO",
            "hora_venezolana": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "ultimo_capturado": {
                "fecha": str(ultimo[0]) if ultimo else None,
                "hora": ultimo[1] if ultimo else None,
                "animal": ultimo[2] if ultimo else None
            },
            "ultima_prediccion": {
                "fecha": str(pred[0]) if pred else None,
                "hora": pred[1] if pred else None,
                "predicho": pred[2] if pred else None,
                "confianza": round(float(pred[3] or 0)) if pred else 0,
                "real": pred[4] if pred else None,
                "acierto": pred[5] if pred else None
            },
            "metricas": {
                "total_predicciones": int(met[0] or 0),
                "calibradas": int(met[1] or 0),
                "aciertos": int(met[2] or 0),
                "efectividad_pct": float(met[3] or 0)
            },
            "historico": {
                "total_registros": int(hist[0] or 0),
                "desde": str(hist[1]) if hist[1] else None,
                "hasta": str(hist[2]) if hist[2] else None
            },
            "predicciones_hoy": hoy,
            "top_deuda_hora_actual": {
                "hora": hora_actual,
                "candidatos": top_deuda
            }
        }
    except Exception as e:
        return {"estado": f"❌ ERROR: {str(e)}"}


# ══════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════
@app.get("/health")
async def health():
    return {"status": "ok", "version": "LOTTOAI PRO V6"}
