from collections import Counter, defaultdict
from sqlalchemy import text
import pandas as pd


async def analizar_estadisticas(db):

    res = await db.execute(text("""
        SELECT fecha, hora, animalito
        FROM historico
        ORDER BY fecha, hora
    """))

    rows = res.fetchall()

    df = pd.DataFrame(rows, columns=["fecha", "hora", "animal"])

    # =====================
    # Frecuencia global
    # =====================
    freq_global = df["animal"].value_counts()

    # =====================
    # Frecuencia por hora
    # =====================
    freq_hora = (
        df.groupby(["hora", "animal"])
        .size()
        .reset_index(name="count")
    )

    # =====================
    # precisión por hora
    # =====================
    hora_stats = {}

    for hora in df["hora"].unique():
        total = len(df[df["hora"] == hora])
        top = (
            df[df["hora"] == hora]["animal"]
            .value_counts()
            .iloc[0]
        )

        precision = top / total if total else 0

        hora_stats[hora] = {
            "total": total,
            "precision": round(precision, 3)
        }

    return {
        "freq_global": freq_global.head(10).to_dict(),
        "hora_stats": hora_stats
    }
