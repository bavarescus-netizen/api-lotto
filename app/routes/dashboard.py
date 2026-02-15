import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. Ubicamos la carpeta 'app' (un nivel arriba de 'routes')
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Apuntamos a 'app/templates'
templates_path = str(BASE_DIR / "templates")

print(f"DEBUG: Buscando plantillas en: {templates_path}")
templates = Jinja2Templates(directory=templates_path)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Usa el nombre EXACTO que ves en GitHub (¿dashboard.html o Dashboard.html?)
    return templates.TemplateResponse("dashboard.html", {"request": request})
