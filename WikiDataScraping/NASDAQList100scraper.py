import pandas as pd
import requests
from bs4 import BeautifulSoup

# 1. URL der Nasdaq-100-Liste
url = "https://en.wikipedia.org/wiki/Nasdaq-100"

# 2. Seiteninhalt abrufen
try:
    # Setze einen User-Agent, um das Risiko einer Blockade zu verringern
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status() #
except requests.exceptions.RequestException as e:
    print(f"Fehler beim Abrufen der URL: {e}")
    exit()

# 3. HTML mit BeautifulSoup parsen
soup = BeautifulSoup(response.text, 'html.parser')

# 4. Die Tabelle gezielt über ihre ID finden
table = soup.find('table', {'id': 'constituents'})

if table is None:
    # FALLBACK-Option: 
    print("constituents nicht gefunden.")
    all_wikitables = soup.find_all('table', {'class': 'wikitable'})
    # Nimm die erste gefundene Tabelle, die mindestens 4 Spalten hat 
    for t in all_wikitables:
        header = t.find('thead')
        if header and len(header.find_all(['th', 'td'])) >= 4:
            table = t
            break
    
    if table is None:
        print("Fehler")
        exit()

# 5. Daten extrahieren und in eine Liste von Listen umwandeln
data = []
if table.tbody:
    rows = table.tbody.find_all('tr')
else:
    rows = table.find_all('tr') # Fallback, falls kein tbody-Tag existiert

for row in rows:
    cols = row.find_all(['td']) 
    if cols and len(cols) >= 2: 
        ticker = cols[0].text.strip()
        company_text = cols[1].text.strip().replace('\xa0', ' ') 
        if ticker:
            data.append([ticker, company_text])

# 6. DataFrame erstellen und speichern
columns = ['Ticker', 'Company']
companies_df = pd.DataFrame(data, columns=columns)

# Bereinigung: Entferne eventuelle leere Ticker und Duplikate
companies_df = companies_df[companies_df['Ticker'].str.len() > 1]
companies_df = companies_df.drop_duplicates().reset_index(drop=True)

companies_df.to_csv('nasdaq_100_constituents.csv', index=False)


print("---")
print(f"Nasdaq-100 Liste ({len(companies_df)} Firmen) erfolgreich extrahiert und in 'nasdaq_100_constituents.csv' gespeichert.")
print(companies_df.head())
print("---")