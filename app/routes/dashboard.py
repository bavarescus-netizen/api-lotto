import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. Obtenemos la ruta de la carpeta 'app' de forma segura
# Como este archivo está en app/routes/, subimos un nivel para llegar a app/
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Ahora le decimos a Jinja2 que busque las plantillas directamente en 'app'
# donde acabas de mover el archivo HTML
templates = Jinja2Templates(directory=str(BASE_DIR))

print(f"DEBUG: Buscando archivos HTML en: {BASE_DIR}")

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # IMPORTANTE: Asegúrate de que el nombre sea exacto (minúsculas/mayúsculas)
    # según cómo lo veas en GitHub. Si es 'Dashboard.html', cámbialo aquí.
    return templates.TemplateResponse("dashboard.html", {"request": request})
