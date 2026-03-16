import pandas as pd
from motor_v10 import generar_prediccion


def backtest(historial):

    top1 = 0
    top3 = 0
    top5 = 0

    total = 0

    historial_predicciones = []

    for i in range(100, len(historial) - 1):

        pasado = historial[:i]

        real = historial[i]

        pred = generar_prediccion(
            pasado,
            historial_predicciones
        )

        p1 = pred["top1"]
        p3 = pred["top3"]
        p5 = pred["top5"]

        if real == p1:
            top1 += 1

        if real in p3:
            top3 += 1

        if real in p5:
            top5 += 1

        historial_predicciones.append(p1)

        total += 1

    return {
        "total_sorteos": total,
        "top1": top1 / total,
        "top3": top3 / total,
        "top5": top5 / total
    }
