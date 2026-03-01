"""
MAIN.PY — Corregido y limpio
- Importaciones unificadas (todas desde db, no app.database)
- Router de predicción conectado
- Scheduler V2
"""

import os
import re
import asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito
from app.services.motor_v5 import generar_prediccion, obtener_estadisticas

app = FastAPI(title="LOTTOAI PRO")

# Routers
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(prediccion.router)
app.include_router(cargarhist.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Archivos estáticos
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")

# Templates
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

@app.on_event("startup")
async def iniciar_bot():
    asyncio.create_task(ciclo_infinito())
    print("🚀 LOTTOAI PRO iniciado — Bot de vigilancia activo")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Predicción del motor V5
        res_ia = await generar_prediccion(db)

        # Estadísticas generales
        stats_data = await obtener_estadisticas(db)

        # Últimos 12 sorteos con resultado de auditoría
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, h.hora DESC
            LIMIT 12
        """)
        res_db = await db.execute(query)

        ultimos_db = []
        for r in res_db.fetchall():
            nombre_animal = re.sub(r'[^a-z]', '', r[2].lower())
            ultimos_db.append({
                "hora": r[1],
                "animal": r[2],
                "img": f"{nombre_animal}.png",
                "acierto": r[3]
            })

        efectividad = stats_data.get("efectividad_global", 0)

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db,
            "efectividad": efectividad,
            "aciertos_hoy": stats_data.get("aciertos_hoy", 0),
            "sorteos_hoy": stats_data.get("sorteos_hoy", 0),
            "total_historico": stats_data.get("total_historico", 0),
            "ultimo_resultado": res_ia.get("ultimo_resultado", "N/A"),
            "analisis": res_ia.get("analisis", "")
        })

    except Exception as e:
        print(f"Error en Home: {e}")
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)

@app.get("/health")
async def health():
    return {"status": "ok", "version": "LOTTOAI PRO V5"}
