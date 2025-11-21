# pip install yfinance pandas
import yfinance as yf
import pandas as pd
import os
import time

# 1. NASDAQ-100 Ticker Liste (Beispiel, du kannst sie erweitern)
nasdaq_100_tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "PEP",
    "CSCO", "INTC", "ADBE", "CMCSA", "AVGO", "TXN", "QCOM"
    # Füge hier die restlichen Ticker hinzu
]

# 2. Ordner zum Speichern der CSV-Dateien
output_folder = "nasdaq100_data"
os.makedirs(output_folder, exist_ok=True)

# 3. Daten abrufen und speichern
for ticker in nasdaq_100_tickers:
    print(f"Lade Daten für {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="max")  # gesamte verfügbare Historie
        if not data.empty:
            file_path = os.path.join(output_folder, f"{ticker}.csv")
            data.to_csv(file_path)
            print(f"{ticker} gespeichert ({len(data)} Zeilen).")
        else:
            print(f"Keine Daten für {ticker} gefunden.")
    except Exception as e:
        print(f"Fehler bei {ticker}: {e}")
    
    # Sicherheitspause, um Rate-Limiting zu vermeiden
    time.sleep(1)  

print("Alle Daten abgerufen und gespeichert.")