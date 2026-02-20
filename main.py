import sys
import os
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

# 1. Ajuste de rutas: Esto le dice a Render que busque en 'app' y en la raíz
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos estáticos y Templates (Tus rutas originales)
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones (Tal como las tienes tú)
from db import get_db
from services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from routes import prediccion, entrenar, stats, historico

app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# 4. Ruta de inicio
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    # Llamada a tus funciones de motor_v4
    res_ia = await generar_prediccion(db)
    bitacora = await obtener_bitacora_avance(db)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "registros": "28,709",
        "top3": res_ia.get("top3", []),
        "decision": res_ia.get("decision", "MOTOR ACTIVO"),
        "bitacora": bitacora
    })

# 5. Ejecución
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
