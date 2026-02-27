import os
import re
from fastapi import FastAPI, Request, Depends, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Conexión con tu base de datos y rutas en la carpeta app/routes
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion

app = FastAPI()

# 1. REGISTRO DE ROUTERS (Para eliminar los errores 404)
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)

# 2. CONFIGURACIÓN DE RUTAS DE ARCHIVOS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Montar imágenes (Raíz -> imagenes)
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")

# Templates (Raíz -> app -> routes)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Llamamos a la lógica de predicción desde prediccion.py
        res_ia = await prediccion.generar_prediccion(db)
        
        # Obtenemos los últimos 12 registros de la auditoría
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """)
        res_db = await db.execute(query)
        
        ultimos_db = []
        for r in res_db.fetchall():
            # Limpiar nombre para la imagen: "02 TORO" -> "toro.png"
            nombre_limpio = re.sub(r'[^a-z]', '', r[2].lower())
            ultimos_db.append({
                "hora": r[1],
                "animal": r[2],
                "img": f"{nombre_limpio}.png",
                "acierto": r[3]
            })

        # SEGURO PARA TABLA METRICS (Evita el Error 500)
        try:
            res_efec = await db.execute(text("SELECT precision FROM metrics WHERE id = 1"))
            metric = res_efec.fetchone()
            efectividad_global = metric[0] if metric else 0.0
        except Exception:
            efectividad_global = 0.0  # Si la tabla no existe aún, muestra 0.0

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db,
            "efectividad": efectividad_global
        })
    except Exception as e:
        return HTMLResponse(content=f"Error Crítico: {str(e)}", status_code=500)
