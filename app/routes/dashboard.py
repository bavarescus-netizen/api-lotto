import os
from pathlib import Path
from fastapi.templating import Jinja2Templates

# 1. Obtenemos la ruta de este archivo (dashboard.py)
current_file = Path(__file__).resolve()

# 2. Buscamos la carpeta raíz del proyecto (donde está 'app' y 'templates')
# Subimos dos niveles: de 'routes' a 'app', y de 'app' a la raíz.
BASE_DIR = current_file.parents[2]

# 3. Construimos la ruta a la carpeta de plantillas
templates_path = os.path.join(BASE_DIR, "templates")

# Debug: Esto imprimirá en los logs de Render la ruta exacta que se está usando
print(f"DEBUG: Buscando plantillas en: {templates_path}")

templates = Jinja2Templates(directory=templates_path)
