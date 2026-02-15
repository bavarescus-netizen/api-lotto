import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. Forzamos la ruta a la misma carpeta donde está ESTE archivo .py
CURRENT_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(CURRENT_DIR))

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # 2. Verificamos qué archivos hay aquí realmente para el log
    archivos_aqui = os.listdir(CURRENT_DIR)
    print(f"DEBUG: Archivos en la carpeta routes: {archivos_aqui}")
    
    # 3. Buscamos el archivo ignorando si tiene mayúsculas o minúsculas
    archivo_a_cargar = "dashboard.html"
    for f in archivos_aqui:
        if f.lower() == "dashboard.html":
            archivo_a_cargar = f
            break

    return templates.TemplateResponse(archivo_a_cargar, {"request": request})
