@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        # 1. PASO PESADO: Aprender de los 28,709 registros
        # Esto calcula qué animales salen más por hora y detecta tendencias
        await db.execute(text("TRUNCATE TABLE probabilidades_hora"))
        
        query_aprendizaje = text("""
            INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
            WITH stats_global AS (
                SELECT EXTRACT(HOUR FROM hora)::INT as h, animalito, COUNT(*) as c
                FROM historico GROUP BY 1, 2
            ),
            stats_reciente AS (
                SELECT EXTRACT(HOUR FROM hora)::INT as h, animalito, COUNT(*) as c
                FROM historico WHERE fecha >= CURRENT_DATE - INTERVAL '15 days' GROUP BY 1, 2
            )
            SELECT g.h, g.animalito, g.c,
                   ((g.c * 0.4) + (COALESCE(r.c, 0) * 0.6)) as peso,
                   CASE WHEN COALESCE(r.c, 0) > 0 THEN 'Caliente' ELSE 'Frío' END
            FROM stats_global g
            LEFT JOIN stats_reciente r ON g.h = r.h AND g.animalito = r.animalito
            WHERE g.h BETWEEN 9 AND 19
        """)
        await db.execute(query_aprendizaje)
        
        # 2. PASO DE AUDITORÍA: Sincronizar aciertos pasados
        resultado_calib = await entrenar_modelo_v4(db)
        
        await db.commit()
        return {
            "status": "success",
            "mensaje": "Cerebro Re-Calibrado con 28,709 registros.",
            "logs": "Tablas de probabilidad y auditoría sincronizadas."
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
