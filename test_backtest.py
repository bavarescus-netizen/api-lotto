import pandas as pd
from services.backtest_motor import backtest


df = pd.read_excel("data/historico.xlsx")

historial = df["animal"].tolist()

resultado = backtest(historial)

print("RESULTADOS")

print("Total sorteos:", resultado["total_sorteos"])

print("TOP1:", round(resultado["top1"] * 100, 2), "%")
print("TOP3:", round(resultado["top3"] * 100, 2), "%")
print("TOP5:", round(resultado["top5"] * 100, 2), "%")
