from sqlalchemy import text
from datetime import datetime

# Mapeo oficial de Lotto Activo
ANIMALES_INFO = {
    "delfin": {"num": "0", "img": "delfin.png"}, "ballena": {"num": "00", "img": "ballena.png"},
    "carnero": {"num": "1", "img": "carnero.png"}, "toro": {"num": "2", "img": "toro.png"},
    "ciempies": {"num": "3", "img": "ciempies.png"}, "alacran": {"num": "4", "img": "alacran.png"},
    "leon": {"num": "5", "img": "leon.png"}, "rana": {"num": "6", "img": "rana.png"},
    "perico": {"num": "7", "img": "perico.png"}, "raton": {"num": "8", "img": "raton.png"},
    "aguila": {"num": "9", "img": "aguila.png"}, "tigre": {"num": "10", "img": "tigre.png"},
    "gato": {"num": "11", "img": "gato.png"}, "caballo": {"num": "12", "img": "caballo.png"},
    "mono": {"num": "13", "img": "mono.png"}, "paloma": {"num": "14", "img": "paloma.png"},
    "zorro": {"num": "15", "img": "zorro.png"}, "oso": {"num": "16", "img": "oso.png"},
    "pavo": {"num": "17", "img": "pavo.png"}, "burro": {"num": "18", "img": "burro.png"},
    "chivo": {"num": "19", "img": "chivo.png"}, "cochino": {"num": "20", "img": "cochino.png"},
    "gallo": {"num": "21", "img": "gallo.png"}, "camello": {"num": "22", "img": "camello.png"},
    "cebra": {"num": "23", "img": "cebra.png"}, "iguana": {"num": "24", "img": "iguana.png"},
    "gallina": {"num": "25", "img": "gallina.png"}, "vaca": {"num": "26", "img": "vaca.png"},
    "perro": {"num": "27", "img": "perro.png"}, "zamuro": {"num": "28", "img": "zamuro.png"},
    "elefante": {"num": "29", "img": "elefante.png"}, "caiman": {"num": "30", "img": "caiman.png"},
    "lapa": {"num": "31", "img": "lapa.png"}, "ardilla": {"num": "32", "img": "ardilla.png"},
    "pescado": {"num": "33", "img": "pescado.png"}, "venado": {"num": "34", "img": "venado.png"},
    "jirafa": {"num": "35", "img": "jirafa.png"}, "culebra": {"num": "36", "img": "culebra.png"}
}

async def generar_prediccion(db):
    ahora = datetime.now()
    hora_actual = ahora.strftime("%I:00 %p")
    
    # 1. Obtener último resultado
    res_ultimo = await db.execute(text("SELECT animalito FROM historico ORDER BY id DESC LIMIT 1"))
    ultimo = res_ultimo.scalar()

    # 2. Consultar Conocimiento V4 (Markov + Frecuencia)
    query = text("""
        SELECT proximo_probable, fuerza 
        FROM conocimiento_v4 
        WHERE animal_actual = :u AND hora = :h
        ORDER BY fuerza DESC LIMIT 3
    """)
    res = await db.execute(query, {"u": ultimo, "h": hora_actual})
    patrones = res.fetchall()

    total_fuerza = sum(p.fuerza for p in patrones) if patrones else 1
    
    # 3. Formatear resultados visuales
    top3_visual = []
    for p in patrones:
        info = ANIMALES_INFO.get(p.proximo_probable, {"num": "??", "img": "default.png"})
        porcentaje = round((p.fuerza / total_fuerza) * 100, 1)
        top3_visual.append({
            "animal": p.proximo_probable.upper(),
            "numero": info["num"],
            "imagen": f"/static/images/{info['img']}",
            "porcentaje": f"{porcentaje}%",
            "fuerza": p.fuerza
        })

    # Decision de rentabilidad
    fuerza_max = patrones[0].fuerza if patrones else 0
    decision = "🟢 JUGAR (ALTA)" if fuerza_max > 7 else "🟡 MODERADO" if fuerza_max > 4 else "🔴 ESPERAR"

    return {"hora": hora_actual, "ultimo": ultimo, "decision": decision, "top3": top3_visual}
