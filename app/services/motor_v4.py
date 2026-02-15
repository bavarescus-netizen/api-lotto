from sqlalchemy import text
from datetime import datetime
import random

async def generar_prediccion(db):
    ahora = datetime.now()
    hora_actual = ahora.strftime("%I:00 %p")
    
    # 1️⃣ OBTENER EL ÚLTIMO RESULTADO PARA LA TRANSICIÓN (MARKOV)
    # Esto es lo que querías: saber qué salió a la 1 para predecir las 2
    res_ultimo = await db.execute(text("""
        SELECT animalito FROM historico 
        ORDER BY fecha DESC, hora DESC LIMIT 1
    """))
    ultimo_animal = res_ultimo.scalar()

    # 2️⃣ EL CEREBRO SQL: PROCESAR 29,000 REGISTROS EN MILISEGUNDOS
    # Esta query calcula: Frecuencia por hora, Atraso y Transición
    query_maestra = text("""
        WITH 
        FrecuenciaHora AS (
            SELECT animalito, COUNT(*) as veces 
            FROM historico WHERE hora = :hora GROUP BY animalito
        ),
        Atraso AS (
            SELECT animalito, 
            (SELECT COUNT(*) FROM historico) - MAX(id) as sorteos_sin_salir
            FROM historico GROUP BY animalito
        ),
        Transicion AS (
            SELECT siguiente, COUNT(*) as fuerza
            FROM (
                SELECT animalito, LEAD(animalito) OVER (ORDER BY id) as siguiente
                FROM historico
            ) AS secuencia
            WHERE animalito = :ultimo
            GROUP BY siguiente
        )
        SELECT 
            a.animalito,
            COALESCE(fh.veces, 0) as peso_hora,
            COALESCE(atr.sorteos_sin_salir, 0) as peso_atraso,
            COALESCE(tr.fuerza, 0) as peso_transicion
        FROM (SELECT DISTINCT animalito FROM historico) a
        LEFT JOIN FrecuenciaHora fh ON a.animalito = fh.animalito
        LEFT JOIN Atraso atr ON a.animalito = atr.animalito
        LEFT JOIN Transicion tr ON a.animalito = tr.siguiente
    """)

    res = await db.execute(query_maestra, {
        "hora": hora_actual, 
        "ultimo": ultimo_animal
    })
    filas = res.fetchall()

    # 3️⃣ ALGORITMO DE PUNTUACIÓN (SCORING)
    # Aquí aplicamos la lógica de "Jugador"
    candidatos = []
    for r in filas:
        nombre, p_hora, p_atraso, p_trans = r
        
        # Fórmula Maestra: 
        # (Frecuencia en esta hora * 0.4) + (Fuerza de lo que salió antes * 0.4) + (Bono por Atraso * 0.2)
        score = (p_hora * 0.4) + (p_trans * 0.4) + (p_atraso * 0.01)
        
        candidatos.append({
            "animal": nombre,
            "score": round(score, 2),
            "detalles": {"hora": p_hora, "trans": p_trans, "atraso": p_atraso}
        })

    # 4️⃣ DECISIÓN DE OPERAR (TU FILTRO DE RENTABILIDAD)
    # Ordenamos por score
    ranking = sorted(candidatos, key=lambda x: x["score"], reverse=True)
    top3 = ranking[:3]
    
    # Calculamos la "Brecha de Confianza"
    # Si el 1ero es mucho mejor que el 4to, hay patrón claro.
    brecha = top3[0]["score"] - ranking[3]["score"]
    
    decision = "ESPERAR"
    if brecha > 2.0: # Umbral ajustable según aprendizaje
        decision = "JUGAR (ALTA CONFIANZA)"
    elif brecha > 1.0:
        decision = "OPERACIÓN MODERADA"

    # 5️⃣ GUARDAR EN TABLA DE PREDICCIONES PARA APRENDER LUEGO
    for p in top3:
        await db.execute(text("""
            INSERT INTO predicciones (fecha, hora, animal, score, acertado)
            VALUES (CURRENT_DATE, :hora, :animal, :score, NULL)
        """), {"hora": hora_actual, "animal": p["animal"], "score": p["score"]})
    
    await db.commit()

    return {
        "hora": hora_actual,
        "ultimo_resultado": ultimo_animal,
        "decision": decision,
        "confianza_gap": round(brecha, 2),
        "top3": top3
    }
