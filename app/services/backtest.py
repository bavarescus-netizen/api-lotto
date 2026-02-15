from sqlalchemy import text
from datetime import datetime
import json

async def entrenar_modelo(db):
    print("🧠 [CEREBRO] Iniciando análisis profundo de 29,000 registros...")

    # 1️⃣ CREAR TABLA DE CONOCIMIENTO SI NO EXISTE
    # Aquí es donde el sistema "guarda lo que aprendió"
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS conocimiento_v4 (
            id SERIAL PRIMARY KEY,
            animal_actual TEXT,
            proximo_probable TEXT,
            fuerza INT,
            hora TEXT,
            ultima_actualizacion TIMESTAMP DEFAULT NOW()
        )
    """))

    # 2️⃣ LIMPIAR CONOCIMIENTO ANTIGUO PARA RE-ENTRENAR
    await db.execute(text("DELETE FROM conocimiento_v4"))

    # 3️⃣ LÓGICA DE SECUENCIAS (EL CORAZÓN DEL APRENDIZAJE)
    # Esta query busca: "Cuando sale X, ¿qué sale después en la siguiente hora?"
    query_secuencia = text("""
        WITH Secuencia AS (
            SELECT 
                animalito as actual, 
                LEAD(animalito) OVER (ORDER BY fecha, hora) as siguiente,
                LEAD(hora) OVER (ORDER BY fecha, hora) as hora_siguiente
            FROM historico
        )
        SELECT actual, siguiente, hora_siguiente, COUNT(*) as ocurrencias
        FROM Secuencia
        WHERE siguiente IS NOT NULL
        GROUP BY actual, siguiente, hora_siguiente
        HAVING COUNT(*) > 3
        ORDER BY ocurrencias DESC
    """)

    res = await db.execute(query_secuencia)
    patrones = res.fetchall()

    # 4️⃣ GUARDAR EL APRENDIZAJE
    for p in patrones:
        await db.execute(text("""
            INSERT INTO conocimiento_v4 (animal_actual, proximo_probable, hora, fuerza)
            VALUES (:act, :sig, :h, :f)
        """), {
            "act": p.actual,
            "sig": p.siguiente,
            "h": p.hora_siguiente,
            "f": p.ocurrencias
        })

    # 5️⃣ REGISTRAR QUE EL ENTRENAMIENTO FUE EXITOSO EN MÉTRICAS
    await db.execute(text("""
        INSERT INTO metricas(total, aciertos, errores, precision, fecha)
        VALUES (:t, 0, 0, 0, :now)
    """), {"t": len(patrones), "now": datetime.now()})

    await db.commit()
    print(f"✅ [CEREBRO] Entrenamiento finalizado. Se detectaron {len(patrones)} patrones de éxito.")
    return {"status": "sabio", "patrones_detectados": len(patrones)}
