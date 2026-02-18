import sys
import os
from fastapi from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
# Importación corregida según tu instrucción
from db import get_db 

router = APIRouter()

@router.get("/procesar")
async def procesar_entrenamiento(db: AsyncSession = Depends(get_db)):
    try:
        # 1. Limpiar predicciones previas
        await db.execute(text("TRUNCATE TABLE probabilidades_hora"))
        
        # 2. SQL con casting ::TIME para evitar el error de 'unknown' en Postgres
        query = text("""
            INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
            WITH stats_global AS (
                SELECT 
                    EXTRACT(HOUR FROM hora::TIME)::INT as h, 
                    animalito, 
                    COUNT(*) as c
                FROM historico 
                GROUP BY 1, 2
            ),
            stats_reciente AS (
                SELECT 
                    EXTRACT(HOUR FROM hora::TIME)::INT as h, 
                    animalito, 
                    COUNT(*) as c
                FROM historico 
                WHERE fecha >= CURRENT_DATE - INTERVAL '15 days' 
                GROUP BY 1, 2
            )
            SELECT 
                g.h, 
                g.animalito, 
                g.c,
                ((g.c * 0.4) + (COALESCE(r.c, 0) * 0.6)) as peso,
                CASE WHEN COALESCE(r.c, 0) > 0 THEN 'Caliente' ELSE 'Frío' END
            FROM stats_global g
            LEFT JOIN stats_reciente r ON g.h = r.h AND g.animalito = r.animalito
            WHERE g.h BETWEEN 9 AND 19
        """)
        
        await db.execute(query)
        await db.commit()
        return {"status": "success", "message": "Motor entrenado correctamente"}
    except Exception as e:
        await db.rollback()
        return {"status": "error", "detail": str(e)}import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas para Render
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# Archivos estáticos
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

# Templates
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# Importación de Routers
from app.routes import prediccion, entrenar, historico, stats

# Conexión de Rutas con prefijo /api (Sincronizado con Dashboard)
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "registros": "28,709"
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
