import os, re, asyncio, datetime
from fastapi import FastAPI, Request, Depends, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

# --- TUS RUTAS ORIGINALES (SIN CAMBIOS) ---
from db import get_db, AsyncSessionLocal
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito
from app.services.motor_v10 import (
    generar_prediccion, obtener_estadisticas, obtener_bitacora,
    entrenar_modelo, backtest, calibrar_predicciones,
    llenar_auditoria_retroactiva, aprender_desde_historico,
    migrar_schema, actualizar_resultados_señales, obtener_score_señales,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="LottoAI PRO V6.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Carga de directorios protegida
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

templates = None
if os.path.exists("templates"):
    templates = Jinja2Templates(directory="templates")

_tarea = {
    "nombre": None,
    "estado": "idle",
    "progreso": "",
    "resultado": None,
    "iniciado": None,
}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Usando 'animalito' exclusivamente (según tu DB)
        res_h = await db.execute(text("""
            SELECT fecha, hora, animalito 
            FROM historico 
            WHERE loteria='Lotto Activo' 
            ORDER BY fecha DESC, hora DESC LIMIT 10
        """))
        ultimos = res_h.fetchall()

        query_stats = text("""
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
            ORDER BY a.hora ASC
        """)
        
        rows = (await db.execute(query_stats)).fetchall()
        
        stats_final = []
        for r in rows:
            total = int(r[1]) if r[1] else 0
            if total > 0:
                stats_final.append({
                    "hora": r[0],
                    "total": total,
                    "aciertos_top1": int(r[2]),
                    "aciertos_top3": int(r[3]),
                    "ef_top1": round((int(r[2])/total)*100, 2),
                    "ef_top3": round((int(r[3])/total)*100, 2)
                })

        if templates:
            return templates.TemplateResponse("index.html", {
                "request": request,
                "ultimos": ultimos,
                "stats": stats_final,
                "tarea": _tarea
            })
        return HTMLResponse("Templates no encontrados.")

    except Exception as e:
        logger.error(f"Error Index: {e}")
        if templates:
            return templates.TemplateResponse("index.html", {
                "request": request, "ultimos": [], "stats": [], "tarea": _tarea, "error_msg": str(e)
            })
        return HTMLResponse(f"Error crítico: {e}")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())
    logger.info("✅ Scheduler V6.1 activo")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
