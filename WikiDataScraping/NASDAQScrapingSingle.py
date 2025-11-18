#Code um die einzelnen Unternehmen aus der Wikidata zu scrapen und mit der 
#bisherigen liste zu mergen uum eine fertige Liste zu haben import requests
#
import requests
import pandas as pd
import time

WIKIDATA_ENDPOINT = 'https://query.wikidata.org/sparql'
WIKIDATA_SEARCH_API = 'https://www.wikidata.org/w/api.php'
USER_AGENT_HEADER = {'User-Agent': 'DataScienceProject/1.0 (Student Project)'}
# Globales, höheres Timeout setzen
GLOBAL_TIMEOUT = 30 

def get_wikidata_qid(company_name):
    """Sucht die Wikidata Q-ID basierend auf dem Firmennamen (schnelle API)."""
    params = {
        'action': 'wbsearchentities',
        'search': company_name,
        'language': 'en',
        'format': 'json',
        'limit': 1,
    }
    try:
        # Hinzufügen des Headers zur Anfrage
        response = requests.get(
            WIKIDATA_SEARCH_API, 
            params=params, 
            headers=USER_AGENT_HEADER, # <<< WICHTIG: Fügt den User-Agent hinzu
            timeout=GLOBAL_TIMEOUT
        )
        response.raise_for_status() # Löst den 403-Fehler aus, wenn er auftritt
        data = response.json()
        
        if data.get('search'):
            return data['search'][0]['id']
            
    except requests.exceptions.HTTPError as e:
        # Spezifische Behandlung für 403-Fehler (zum Debuggen)
        print(f"  ❌ HTTP-Fehler ({e.response.status_code}) bei QID-Suche für {company_name}. Ursache: Vermutlich fehlender/blockierter User-Agent.")
    except Exception as e:
        print(f"  Fehler bei der QID-Suche für {company_name}: {e}")
    return None

def fetch_wikidata_metadata_by_qid(qid, company_name):
    """Ruft Metadaten mit der exakten Q-ID über SPARQL ab (schnell)."""
    sparql_query = f"""
    SELECT ?item ?inception ?industryLabel ?website WHERE {{
      VALUES ?item {{ wd:{qid} }}
      
      OPTIONAL {{ ?item wdt:P571 ?inception. }}
      OPTIONAL {{ ?item wdt:P452 ?industry. }}
      OPTIONAL {{ ?item wdt:P856 ?website. }}
      
      SERVICE wikibase:label {{ 
        bd:serviceParam wikibase:language "en". 
        ?industry rdfs:label ?industryLabel.
      }}
    }}
    """
    
    try:
        response = requests.get(
            WIKIDATA_ENDPOINT,
            params={'query': sparql_query, 'format': 'json'},
            headers={'Accept': 'application/sparql-results+json', 'User-Agent': 'DataScienceProject/1.0'},
            timeout=GLOBAL_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        bindings = data['results']['bindings']
        
        if bindings:
            result = bindings[0]
            
            # Datum und Jahr extrahieren
            inception_date = result.get('inception', {}).get('value', '')
            inception_year = inception_date.split('-')[0] if inception_date else 'N/A'
            
            return {
                'Wikidata_ID': qid,
                'Industry': result.get('industryLabel', {}).get('value', 'N/A'),
                'Founding_Year': inception_year,
                'Website': result.get('website', {}).get('value', 'N/A')
            }
            
    except Exception as e:
        print(f"  Fehler bei der SPARQL-Abfrage für QID {qid}: {e}")
    
    return {'Wikidata_ID': qid, 'Industry': 'N/A', 'Founding_Year': 'N/A', 'Website': 'N/A'}

# 3. Haupt-Loop (Wiederholungsversuche eingebaut)
def process_companies(companies_df):
    metadata_list = []
    
    for index, row in companies_df.iterrows():
        company_name = row['Company']
        ticker = row['Ticker']
        
        print(f"-> Verarbeite {company_name} ({ticker})...")
        
        # 1. QID finden
        qid = get_wikidata_qid(company_name)
        
        if qid:
            # 2. Metadaten mit QID abfragen (mit Retry)
            metadata = fetch_wikidata_metadata_by_qid(qid, company_name)
        else:
            print(f"   [SKIP] Konnte keine QID für {company_name} finden.")
            metadata = {'Wikidata_ID': 'N/A', 'Industry': 'N/A', 'Founding_Year': 'N/A', 'Website': 'N/A'}
            
        metadata['Ticker'] = ticker
        metadata['Company'] = company_name
        metadata_list.append(metadata)
        
        # Wichtig: Kurze Pause, um Server nicht zu überlasten
        time.sleep(1) 
    
    return pd.DataFrame(metadata_list)

# Beispiel: Laden Sie Ihr DataFrame und führen Sie die Verarbeitung aus
companies_df = pd.read_csv('/Users/lania/VSCode/DataScience/wikinasdaq_100_constituents.csv') 
metadata_df = process_companies(companies_df)

# Die ursprüngliche Liste mit den neuen Metadaten zusammenführen
final_df = companies_df.merge(metadata_df, on=['Ticker', 'Company'], how='left')

# Stelle sicher, dass die Spalten in einer sinnvollen Reihenfolge sind
final_df = final_df[['Company', 'Ticker', 'Website', 'Industry', 'Founding_Year', 'Wikidata_ID']]

final_df.to_csv("WikiNasdaq_100_constituents.csv", index=False)
print("\n[SUCCESS] Erweiterte Firmendaten wurden in 'WikiNasdaq_100_constituents.csv' gespeichert.")