import os
import re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Importaciones siguiendo tu estructura de carpetas exacta
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion

app = FastAPI(title="LOTTOAI PRO")

# Registro de rutas (Routers)
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Montaje de estáticos e imágenes
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")

# Configuración de Templates apuntando a app/routes
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # 1. Obtener Predicciones (Top 3)
        res_ia = await prediccion.generar_prediccion(db)
        
        # 2. Obtener Auditoría (Últimos 12)
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """)
        res_db = await db.execute(query)
        
        ultimos_db = []
        for r in res_db.fetchall():
            # Limpieza de nombre para la imagen (ej: "03 CIEMPIES" -> "ciempies.png")
            nombre_img = re.sub(r'[^a-z]', '', r[2].lower())
            ultimos_db.append({
                "hora": r[1],
                "animal": r[2],
                "img": f"{nombre_img}.png",
                "acierto": r[3]
            })

        # 3. Obtener Métricas (Seguro contra tabla vacía)
        try:
            res_met = await db.execute(text("SELECT precision FROM metrics WHERE id = 1"))
            metric = res_met.fetchone()
            efectividad = metric[0] if metric else 0.0
        except:
            efectividad = 0.0

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db,
            "efectividad": efectividad
        })
    except Exception as e:
        return HTMLResponse(content=f"Error en Home: {str(e)}", status_code=500)
