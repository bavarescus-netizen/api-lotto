import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. Forzamos la ruta a la carpeta 'app' que está un nivel arriba de 'routes'
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR))

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # 2. Verificamos si el archivo existe en esa carpeta antes de lanzarlo
    # Intentamos con minúscula y con mayúscula por si acaso
    posibles_nombres = ["dashboard.html", "Dashboard.html"]
    archivo_final = None
    
    for nombre in posibles_nombres:
        if os.path.exists(BASE_DIR / nombre):
            archivo_final = nombre
            break
    
    if not archivo_final:
        return HTMLResponse(content=f"Error: No encontré el archivo en {BASE_DIR}. Archivos ahí: {os.listdir(BASE_DIR)}", status_code=404)

    return templates.TemplateResponse(archivo_final, {"request": request})
