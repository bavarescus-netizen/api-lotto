from sqlalchemy import text
from datetime import datetime

async def generar_prediccion(db):
    ahora = datetime.now()
    hora_actual = ahora.strftime("%I:00 %p")
    
    # 1️⃣ IDENTIFICAR EL ÚLTIMO RESULTADO (EL DISPARADOR)
    res_ultimo = await db.execute(text("""
        SELECT animalito FROM historico 
        ORDER BY fecha DESC, id DESC LIMIT 1
    """))
    ultimo_animal = res_ultimo.scalar()

    if not ultimo_animal:
        return {"error": "No hay datos históricos para iniciar"}

    # 2️⃣ CONSULTAR EL CONOCIMIENTO APRENDIDO (MARKOV + FRECUENCIA)
    # Buscamos en la tabla de entrenamiento qué es lo más probable después de 'ultimo_animal'
    query_inteligente = text("""
        SELECT proximo_probable, fuerza 
        FROM conocimiento_v4 
        WHERE animal_actual = :ultimo AND hora = :hora
        ORDER BY fuerza DESC LIMIT 3
    """)
    
    res = await db.execute(query_inteligente, {"ultimo": ultimo_animal, "hora": hora_actual})
    patrones = res.fetchall()

    # 3️⃣ CÁLCULO DE CONFIANZA PARA RENTABILIDAD
    # Si no hay patrones fuertes (> 5 ocurrencias), el sistema sugiere precaución
    max_fuerza = patrones[0].fuerza if patrones else 0
    
    if max_fuerza >= 8:
        decision = "🟢 JUGAR - ALTA CONFIANZA"
    elif max_fuerza >= 5:
        decision = "🟡 JUGAR - CONFIANZA MEDIA"
    else:
        decision = "🔴 ESPERAR - PATRÓN DÉBIL"

    # 4️⃣ FORMATEAR TOP 3 PARA EL DASHBOARD
    top3 = []
    for p in patrones:
        top3.append({
            "animal": p.proximo_probable,
            "score": p.fuerza,
            "probabilidad": f"Basado en {p.fuerza} repeticiones"
        })

    # Si no hay suficientes patrones, rellenamos con frecuencia general de la hora
    if len(top3) < 3:
        res_respaldo = await db.execute(text("""
            SELECT animalito, COUNT(*) as c FROM historico 
            WHERE hora = :hora GROUP BY animalito ORDER BY c DESC LIMIT :lim
        """), {"hora": hora_actual, "lim": 3 - len(top3)})
        for r in res_respaldo:
            top3.append({"animal": r[0], "score": r[1], "probabilidad": "Frecuencia Hora"})

    return {
        "hora": hora_actual,
        "despues_de": ultimo_animal,
        "decision": decision,
        "fuerza_patron": max_fuerza,
        "top3": top3
    }
