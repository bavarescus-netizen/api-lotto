import os
import re
from fastapi import FastAPI, Request, Depends, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# IMPORTANTE: Conexión con tus archivos en la carpeta /routes
from db import get_db
from routes import entrenar, stats, historico  # Asegúrate que estos archivos tengan "router = APIRouter()"

app = FastAPI()

# 1. Registro de rutas para que los botones funcionen (Fin del Error 404)
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)

# 2. Configuración de Archivos Estáticos (Imágenes y Templates)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "routes"))

# 3. Función para obtener predicciones (Llamada al motor)
from app.services.motor_v4 import generar_prediccion

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Generamos el Top 3
        res_ia = await generar_prediccion(db)
        
        # Consultamos los últimos 12 resultados para la auditoría
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """)
        res_db = await db.execute(query)
        
        ultimos_db = []
        for r in res_db.fetchall():
            # Limpiamos el nombre para la imagen (ej: "01 CARNERO" -> "carnero.png")
            nombre_limpio = re.sub(r'[^a-z]', '', r[2].lower())
            ultimos_db.append({
                "hora": r[1],
                "animal": r[2],
                "img": f"{nombre_limpio}.png",
                "acierto": r[3]
            })

        # Calculamos la efectividad real
        res_efec = await db.execute(text("SELECT total, precision FROM metrics WHERE id = 1"))
        metric = res_efec.fetchone()
        efectividad_global = metric[1] if metric else 0.0

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db,
            "efectividad": efectividad_global
        })
    except Exception as e:
        print(f"Error en Home: {e}")
        return HTMLResponse(content="Error interno del servidor", status_code=500)
