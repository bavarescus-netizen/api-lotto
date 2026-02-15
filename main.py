import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4")

# 1. Montar archivos estáticos (Tus PNG en la carpeta 'imagenes')
# Ahora /static/aguila.png buscará en la carpeta imagenes/
app.mount("/static", StaticFiles(directory="imagenes"), name="static")

# 2. Configurar el motor de plantillas para el Dashboard
# Busca el archivo dashboard.html dentro de app/templates/
templates = Jinja2Templates(directory="app/templates")

# Importación de routers (después de inicializar app)
from app.routes import prediccion, entrenar, historico, stats

app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

# 3. RUTA CORREGIDA: Ahora carga el Dashboard PRO
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # Esto busca el archivo app/templates/dashboard.html
    return templates.TemplateResponse("dashboard.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
