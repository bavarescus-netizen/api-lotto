import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# Parche de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4")
from fastapi.staticfiles import StaticFiles

# Esto le dice a Render: "Cuando alguien pida /static, busca dentro de la carpeta imagenes"
app.mount("/static", StaticFiles(directory="imagenes"), name="static")
# Importación de routers
from app.routes import prediccion, entrenar, historico, stats

app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return "<h1>API Lotto AI v4 Funcionando</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
    
