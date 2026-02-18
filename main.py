import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Asegurar que el directorio raíz esté en el path para 'from db import get_db'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# Carpeta de imágenes (logos/animalitos)
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

# Templates: Ubicados en app/routes como pediste
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# Importación de Routers
from app.routes import prediccion, entrenar, stats, historico

# Registro de rutas con prefijo unificado
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"]) # Acceso: /api/procesar
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Data"])

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "sync": "2018-2026"
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))import sys
import os
from fastapi import FastAPI, Request
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
