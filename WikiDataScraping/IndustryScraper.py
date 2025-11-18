import pandas as pd
import trafilatura
import json
import time
import os
from urllib.parse import quote

# ==============================================================================
# 1. KONFIGURATION
# ==============================================================================

# Eingabedatei, die die Branchen enthält
INPUT_CSV_FILE = '/Users/lania/VSCode/DataScience/WikiNasdaq_100_constituents.csv'

# Ausgabedatei für die extrahierten Branchenbeschreibungen
OUTPUT_JSON_FILE = 'industry_descriptions.json'

# Basis-URL für die englische Wikipedia
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/wiki/"

# ==============================================================================
# 2. FUNKTION ZUM SCRAPEN EINER EINZELNEN WIKIPEDIA-SEITE
# ==============================================================================

def scrape_industry_page(industry_name):
    """
    Sucht die Wikipedia-Seite für eine bestimmte Branche und extrahiert den Text.
    """
    if not industry_name or industry_name.lower() == 'n/a':
        return None

    # Bereitet den Branchennamen für die URL vor (z.B. "software industry" -> "software_industry")
    formatted_name = industry_name.replace(' ', '_')
    # Stellt sicher, dass Sonderzeichen korrekt kodiert werden
    url_path = quote(formatted_name)
    url = f"{WIKIPEDIA_BASE_URL}{url_path}"

    print(f"   -> Versuche, Text von {url} zu extrahieren...")

    try:
        # Lade den HTML-Inhalt von der URL herunter
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            print(f"   [FAIL] Konnte die Seite für '{industry_name}' nicht herunterladen.")
            return None

        # Extrahiere den Haupttext mit hoher Präzision
        main_text = trafilatura.extract(downloaded, include_comments=False, favor_precision=True)

        if main_text and len(main_text) > 100:
            print(f"   [SUCCESS] {len(main_text)} Zeichen für '{industry_name}' extrahiert.")
            return main_text
        else:
            print(f"   [SKIP] Nicht genügend relevanter Text für '{industry_name}' gefunden.")
            return None

    except Exception as e:
        print(f"   [ERROR] Unerwarteter Fehler bei '{industry_name}': {e}")
        return None

# ==============================================================================
# 3. HAUPTPROGRAMM
# ==============================================================================

if __name__ == "__main__":
    print("[INFO] Starte den Scraper für Branchenbeschreibungen...")
    df = pd.read_csv(INPUT_CSV_FILE)

    # --- KORREKTUR: Verarbeite kommagetrennte Branchenlisten ---
    # 1. Erstelle eine leere Menge, um alle einzigartigen Branchen zu speichern
    all_industries = set()

    # 2. Iteriere durch die 'Industry'-Spalte, spalte die Strings und füge sie zur Menge hinzu
    for item in df['Industry'].dropna():
        # Spalte den String am Komma und entferne Leerzeichen vor/nach jeder Branche
        industries = [industry.strip() for industry in item.split(',')]
        all_industries.update(industries)

    # 3. Entferne 'N/A' und konvertiere die Menge in eine sortierte Liste für konsistente Reihenfolge
    unique_industries = sorted([ind for ind in all_industries if ind.lower() != 'n/a'])

    industry_descriptions = {}

    for industry in unique_industries:
        print(f"\nVerarbeite Branche: '{industry}'")
        description = scrape_industry_page(industry)
        if description:
            industry_descriptions[industry] = description
        time.sleep(1) # Kurze Pause, um die Server nicht zu überlasten

    # Speichere das Dictionary als JSON-Datei
    with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(industry_descriptions, f, ensure_ascii=False, indent=4)

    print(f"\n[SUCCESS] {len(industry_descriptions)} Branchenbeschreibungen wurden in '{OUTPUT_JSON_FILE}' gespeichert.")