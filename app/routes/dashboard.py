import os
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()

# 1. Buscamos la raíz del proyecto de forma dinámica
BASE_DIR = Path(__file__).resolve().parents[2]

# 2. Función para encontrar la carpeta 'templates' donde sea que esté
def find_templates_dir(root):
    for path in root.rglob('templates'):
        if path.is_dir():
            return str(path)
    return str(root) # Si no la encuentra, usa la raíz

templates_path = find_templates_dir(BASE_DIR)
print(f"DEBUG: Jinja2 usará esta ruta: {templates_path}")

templates = Jinja2Templates(directory=templates_path)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # 3. Lista los archivos para ver qué nombre tienen realmente (Mayúsculas/Minúsculas)
    archivos_reales = os.listdir(templates_path)
    print(f"DEBUG: Archivos encontrados en la carpeta: {archivos_reales}")
    
    # Buscamos el archivo ignorando mayúsculas
    nombre_archivo = "dashboard.html"
    for f in archivos_reales:
        if f.lower() == "dashboard.html":
            nombre_archivo = f
            break
            
    return templates.TemplateResponse(nombre_archivo, {"request": request})
