import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# 1. Definimos el router para que main.py lo pueda importar
router = APIRouter()

# 2. Configuración de rutas para las plantillas
current_file = Path(__file__).resolve()
# Subimos dos niveles: de 'routes' a 'app', y de 'app' a la raíz.
BASE_DIR = current_file.parents[2]
templates_path = os.path.join(BASE_DIR, "templates")

# Debug para ver la ruta en los logs de Render
print(f"DEBUG: Buscando plantillas en: {templates_path}")

templates = Jinja2Templates(directory=templates_path)

# 3. Definimos la ruta del Dashboard
@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # IMPORTANTE: Verifica si tu archivo es 'dashboard.html' o 'Dashboard.html'
    # Linux en Render es sensible a las mayúsculas.
    return templates.TemplateResponse("dashboard.html", {"request": request})
