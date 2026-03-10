import os, re, asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion, cargarhist
from app.core.scheduler import ciclo_infinito
from app.services.motor_v5 import (
    generar_prediccion, obtener_estadisticas, obtener_bitacora,
    entrenar_modelo, backtest, calibrar_predicciones,
    llenar_auditoria_retroactiva, aprender_desde_historico,
    obtener_pesos_actuales, migrar_schema,
)

app = FastAPI(title="LOTTOAI PRO V9")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
    allow_credentials=False, allow_methods=["GET","POST"], allow_headers=["*"])
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(prediccion.router)
app.include_router(cargarhist.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR,"imagenes")), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR,"app","routes"))


@app.on_event("startup")
async def iniciar_bot():
    async for db in get_db():
        await migrar_schema(db)  # ← V9: añade columnas pred1/pred2/pred3 si no existen
        try:
            await db.execute(text("""
                ALTER TABLE auditoria_ia
                ADD CONSTRAINT IF NOT EXISTS auditoria_fecha_hora_unique UNIQUE (fecha,hora)
            """))
            await db.commit()
        except Exception:
            await db.rollback()
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS motor_pesos (
                    id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(),
                    peso_reciente FLOAT DEFAULT 0.30, peso_deuda FLOAT DEFAULT 0.25,
                    peso_anti FLOAT DEFAULT 0.25, peso_patron FLOAT DEFAULT 0.10,
                    peso_secuencia FLOAT DEFAULT 0.10, efectividad FLOAT DEFAULT 0.0,
                    total_evaluados INT DEFAULT 0, aciertos INT DEFAULT 0, generacion INT DEFAULT 1
                )
            """))
            res = await db.execute(text("SELECT COUNT(*) FROM motor_pesos"))
            if (res.scalar() or 0) == 0:
                await db.execute(text("""
                    INSERT INTO motor_pesos
                        (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia,efectividad,generacion)
                    VALUES (0.30,0.25,0.25,0.10,0.10,4.2,1)
                """))
            await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"Warning motor_pesos: {e}")
        break
    asyncio.create_task(ciclo_infinito())
    print("🚀 LOTTOAI PRO V9 — Ciclos, Top3, Rentabilidad por hora")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia     = await generar_prediccion(db)
        stats_data = await obtener_estadisticas(db)
        res_db = await db.execute(text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto, a.animal_predicho
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha=a.fecha AND h.hora=a.hora
            WHERE h.loteria='Lotto Activo'
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """))
        ultimos_db = []
        for r in res_db.fetchall():
            nombre_animal = re.sub(r'[^a-z]','',r[2].lower())
            predicho_raw  = re.sub(r'[^a-z]','',(r[4] or '').lower())
            fecha_str     = r[0].strftime("%m-%d") if r[0] else "—"
            ultimos_db.append({
                "fecha": fecha_str, "hora": r[1], "animal": r[2],
                "img": f"{nombre_animal}.png", "acierto": r[3], "predicho": predicho_raw,
            })
        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3",[]),
            "ultimos_db": ultimos_db,
            "efectividad": stats_data.get("efectividad_global",0),
            "efectividad_top3": stats_data.get("efectividad_top3",0),
            "aciertos_hoy": stats_data.get("aciertos_hoy",0),
            "sorteos_hoy": stats_data.get("sorteos_hoy",0),
            "total_historico": stats_data.get("total_historico",0),
            "horas_rentables": stats_data.get("horas_rentables",[]),
            "ultimo_resultado": res_ia.get("ultimo_resultado","N/A"),
            "analisis": res_ia.get("analisis",""),
            "confianza_idx": res_ia.get("confianza_idx",0),
            "señal_texto": res_ia.get("señal_texto",""),
            "hora_premium": res_ia.get("hora_premium",False),
            "ef_hora_top3": res_ia.get("efectividad_hora_top3",0),
        })
    except Exception as e:
        return HTMLResponse(content=f"<h2>Error: {str(e)}</h2>", status_code=500)


@app.get("/historial")
async def get_historial(fecha:str=None, resultado:str=None, animal:str=None,
                         limit:int=200, db:AsyncSession=Depends(get_db)):
    try:
        conditions = ["h.animalito IS NOT NULL","h.loteria='Lotto Activo'",
                      "a.prediccion_1 IS NOT NULL"]
        params = {"limit": limit}
        if fecha:
            conditions.append("a.fecha=:fecha"); params["fecha"] = fecha
        if animal:
            conditions.append("(a.prediccion_1 ILIKE :animal OR a.prediccion_2 ILIKE :animal OR a.prediccion_3 ILIKE :animal OR h.animalito ILIKE :animal)")
            params["animal"] = f"%{animal}%"
        if resultado == "win":
            conditions.append("h.animalito IN (COALESCE(a.prediccion_1,'__'),COALESCE(a.prediccion_2,'__'),COALESCE(a.prediccion_3,'__'))")
        elif resultado == "fail":
            conditions.append("h.animalito NOT IN (COALESCE(a.prediccion_1,'__'),COALESCE(a.prediccion_2,'__'),COALESCE(a.prediccion_3,'__'))")
        where = " AND ".join(conditions)
        rows = await db.execute(text(f"""
            SELECT a.fecha,a.hora,a.prediccion_1,a.prediccion_2,a.prediccion_3,
                a.confianza_pct,h.animalito AS resultado,a.es_hora_rentable,
                CASE WHEN h.animalito IN (
                    COALESCE(a.prediccion_1,'__'),COALESCE(a.prediccion_2,'__'),
                    COALESCE(a.prediccion_3,'__')) THEN true ELSE false END AS acierto
            FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE {where} ORDER BY a.fecha DESC,a.hora DESC LIMIT :limit
        """), params)
        predicciones = [{
            "fecha": r[0].strftime("%Y-%m-%d") if r[0] else "—",
            "hora": str(r[1]) if r[1] else "—",
            "pred1": r[2] or "—", "pred2": r[3] or "—", "pred3": r[4] or "—",
            "confianza": round(float(r[5]),1) if r[5] else None,
            "resultado": r[6] or "—",
            "hora_rentable": bool(r[7]) if r[7] is not None else False,
            "acierto": bool(r[8]),
        } for r in rows.fetchall()]
        total = len(predicciones)
        aciertos = sum(1 for p in predicciones if p["acierto"])
        return {"predicciones": predicciones, "stats": {
            "total": total, "aciertos": aciertos,
            "fallos": total-aciertos,
            "efectividad": round(aciertos/total*100,2) if total>0 else 0}}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error":str(e),"predicciones":[],"stats":{}})


@app.get("/rentabilidad")
async def get_rentabilidad(db: AsyncSession = Depends(get_db)):
    """Análisis de rentabilidad por hora. Pago 1:30, top3, umbral 3.33%"""
    try:
        res = await db.execute(text("""
            SELECT hora,total_sorteos,efectividad_top1,efectividad_top3,
                   es_rentable,ultima_actualizacion
            FROM rentabilidad_hora ORDER BY efectividad_top3 DESC
        """))
        horas = []
        for r in res.fetchall():
            ef3 = float(r[3])
            horas.append({
                "hora": r[0], "total_sorteos": int(r[1]),
                "efectividad_top1": float(r[2]), "efectividad_top3": ef3,
                "es_rentable": bool(r[4]),
                "ventaja_pct": round(ef3 - 3.33, 2),
                "ganancia_x1": round(ef3/100*30 - (1-ef3/100), 2),
                "señal": "✅ OPERAR" if bool(r[4]) else ("⚠️ MARGINAL" if ef3 > 2.5 else "❌ NO OPERAR"),
            })
        rentables = [h for h in horas if h["es_rentable"]]
        return {
            "umbral_minimo": 3.33, "pago_loteria": 30, "n_animales": 3,
            "horas_rentables": len(rentables),
            "mejor_hora": horas[0] if horas else None,
            "detalle": horas,
            "resumen": (f"{len(rentables)}/{len(horas)} horas rentables. "
                        f"Mejor: {horas[0]['hora']} ({horas[0]['efectividad_top3']}%)" if horas else "Sin datos"),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/cargar-ultimo")
async def cargar_ultimo(db: AsyncSession = Depends(get_db)):
    try:
        from app.routes.cargarhist import capturar_ultimo_resultado
        resultado = await capturar_ultimo_resultado(db)
        return {"message": f"✅ Capturado: {resultado}", "ok": True}
    except ImportError:
        return {"message": "⚠️ capturar_ultimo_resultado no expuesta en cargarhist", "ok": False}
    except Exception as e:
        return {"message": f"❌ Error: {str(e)}", "ok": False}


@app.get("/procesar")
async def procesar(db: AsyncSession = Depends(get_db)):
    return await entrenar_modelo(db)


@app.get("/aprender")
async def aprender(desde: str = None, db: AsyncSession = Depends(get_db)):
    from datetime import date
    fecha_inicio = None
    if desde:
        try: fecha_inicio = date.fromisoformat(desde)
        except: return {"error": "Formato inválido. Use YYYY-MM-DD"}
    return await aprender_desde_historico(db, fecha_inicio)


@app.get("/pesos")
async def ver_pesos(db: AsyncSession = Depends(get_db)):
    try:
        res = await db.execute(text("""
            SELECT id,fecha,peso_reciente,peso_deuda,peso_anti,peso_patron,
                   peso_secuencia,efectividad,total_evaluados,aciertos,generacion
            FROM motor_pesos ORDER BY id DESC LIMIT 10
        """))
        return {"historial_pesos": [{"generacion":r[10],"fecha":str(r[1]),
            "pesos":{"reciente":r[2],"deuda":r[3],"anti":r[4],"patron":r[5],"secuencia":r[6]},
            "efectividad":r[7],"total":r[8],"aciertos":r[9]} for r in res.fetchall()]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/retroactivo")
async def retroactivo(desde:str=None,hasta:str=None,dias:int=30,db:AsyncSession=Depends(get_db)):
    from datetime import date
    fd=fh=None
    if desde:
        try: fd=date.fromisoformat(desde)
        except: return {"error":"Formato 'desde' inválido"}
    if hasta:
        try: fh=date.fromisoformat(hasta)
        except: return {"error":"Formato 'hasta' inválido"}
    if fd and fh and (fh-fd).days>366: return {"error":"Rango máximo 1 año"}
    return await llenar_auditoria_retroactiva(db,fd,fh,dias)


@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    return {"stats": await obtener_estadisticas(db), "bitacora_hoy": await obtener_bitacora(db)}


@app.get("/backtest")
async def run_backtest(desde:str,hasta:str,db:AsyncSession=Depends(get_db)):
    from datetime import date
    try:
        fd=date.fromisoformat(desde); fh=date.fromisoformat(hasta)
        if (fh-fd).days>180: return {"error":"Rango máximo: 6 meses"}
        return await backtest(db,fd,fh,max_sorteos=100)
    except ValueError:
        return {"error":"Formato inválido. Use YYYY-MM-DD"}


@app.get("/estado")
async def estado_sistema(db: AsyncSession = Depends(get_db)):
    try:
        u = (await db.execute(text(
            "SELECT fecha,hora,animalito FROM historico WHERE loteria='Lotto Activo' "
            "ORDER BY fecha DESC,hora DESC LIMIT 1"))).fetchone()
        p = (await db.execute(text(
            "SELECT fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto,"
            "prediccion_1,prediccion_2,prediccion_3 FROM auditoria_ia "
            "ORDER BY fecha DESC,hora DESC LIMIT 1"))).fetchone()
        met = (await db.execute(text("""
            SELECT COUNT(*),COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::numeric/
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100,1)
            FROM auditoria_ia"""))).fetchone()
        hist = (await db.execute(text(
            "SELECT COUNT(*),MIN(fecha),MAX(fecha) FROM historico WHERE loteria='Lotto Activo'"))).fetchone()
        rent = (await db.execute(text(
            "SELECT hora,efectividad_top3 FROM rentabilidad_hora "
            "WHERE es_rentable=TRUE ORDER BY efectividad_top3 DESC"))).fetchall()
        import pytz; from datetime import datetime
        ahora = datetime.now(pytz.timezone('America/Caracas'))
        pesos = await obtener_pesos_actuales(db)
        gen   = (await db.execute(text("SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"))).scalar() or 1
        return {
            "estado": "✅ SISTEMA ACTIVO — Motor V9",
            "hora_venezolana": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "motor": {"version":"V9","generacion":gen,"pesos":pesos},
            "ultimo_capturado": {"fecha":str(u[0]),"hora":u[1],"animal":u[2]} if u else {},
            "ultima_prediccion": {"fecha":str(p[0]),"hora":p[1],"pred1":p[6],"pred2":p[7],
                "pred3":p[8],"confianza":round(float(p[3] or 0)),"real":p[4],"acierto":p[5]} if p else {},
            "metricas": {"total":int(met[0] or 0),"calibradas":int(met[1] or 0),
                "aciertos":int(met[2] or 0),"efectividad":float(met[3] or 0)},
            "historico": {"total":int(hist[0] or 0),"desde":str(hist[1]),"hasta":str(hist[2])},
            "horas_rentables": [{"hora":r[0],"ef_top3":float(r[1])} for r in rent],
        }
    except Exception as e:
        return {"estado": f"❌ ERROR: {str(e)}"}


@app.get("/health")
async def health():
    return {"status":"ok","version":"LOTTOAI PRO V9"}
