import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. BASE_DIR apunta a la raíz del proyecto (/opt/render/project/src)
BASE_DIR = Path(__file__).resolve().parents[2]
templates_path = os.path.join(BASE_DIR, "templates")

templates = Jinja2Templates(directory=templates_path)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Listamos para estar seguros de qué hay en la raíz
    print(f"DEBUG: Buscando en {templates_path}")
    if os.path.exists(templates_path):
        print(f"DEBUG: Contenido de templates: {os.listdir(templates_path)}")
    else:
        print("DEBUG: La carpeta templates NO EXISTE en la raíz")

    return templates.TemplateResponse("dashboard.html", {"request": request})
