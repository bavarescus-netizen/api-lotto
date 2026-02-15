from sqlalchemy import text
from datetime import datetime

async def entrenar_modelo(db):
    # Crear tabla de conocimiento
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS conocimiento_v4 (
            id SERIAL PRIMARY KEY, animal_actual TEXT, proximo_probable TEXT, 
            fuerza INT, hora TEXT, ultima_actualizacion TIMESTAMP DEFAULT NOW()
        )
    """))
    await db.execute(text("DELETE FROM conocimiento_v4"))

    # Analizar secuencias de 29,000 registros
    query = text("""
        WITH Secuencia AS (
            SELECT animalito as actual, LEAD(animalito) OVER (ORDER BY id) as siguiente,
            LEAD(hora) OVER (ORDER BY id) as hora_sig FROM historico
        )
        SELECT actual, siguiente, hora_sig, COUNT(*) as f
        FROM Secuencia WHERE siguiente IS NOT NULL
        GROUP BY actual, siguiente, hora_sig HAVING COUNT(*) > 2
        ORDER BY f DESC
    """)
    
    patrones = (await db.execute(query)).fetchall()
    for p in patrones:
        await db.execute(text("""
            INSERT INTO conocimiento_v4 (animal_actual, proximo_probable, hora, fuerza)
            VALUES (:a, :s, :h, :f)
        """), {"a": p.actual, "s": p.siguiente, "h": p.hora_sig, "f": p.f})
    
    await db.commit()
    return {"status": "ok", "patrones": len(patrones)}
