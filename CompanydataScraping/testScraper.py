import trafilatura
import pandas as pd
from playwright.sync_api import sync_playwright
import time

import re
import pandas as pd
from playwright.sync_api import sync_playwright
import trafilatura
import time

def crawl_multi_target_data(company_name, ticker):
    """Sammelt Textdaten von den wahrscheinlichen Haupt-Textquellen (About, Newsroom)."""
    
    # --- 1. Robuste Ableitung der Basis-Domain ---
    
    # a) Entferne Klammern (z.B. Class A) und Leerzeichen an den Enden
    base_name_temp = company_name.split('(')[0].strip() 
    
    # b) Entferne gängige Suffixe wie 'Inc.', 'Co.', 'Corp.'
    # Der Ausdruck ersetzt " Inc.", ", Inc." oder " Inc" am Ende, gefolgt von optionalem Punkt.
    base_name_temp = re.sub(r'[\s\.\,](inc|co|corp|ltd)\.?$', '', base_name_temp, flags=re.IGNORECASE).strip()
    
    # c) Entferne alle verbleibenden Leerzeichen, Punkte und Apostrophe und lowercase
    clean_name = re.sub(r'[\s\.\']', '', base_name_temp).lower()

    # d) Spezialfälle behandeln (da nicht alle Firmen *ihren* Namen als Domain haben)
    if clean_name == "apple":
        base_domain = "https://www.apple.com"
    elif clean_name == "alphabet":
        base_domain = "https://abc.xyz" # Alphabet's Domain
    elif clean_name == "microsoft":
        base_domain = "https://www.microsoft.com"
    else:
        # Generische Ableitung für die meisten Firmen
        base_domain = f"https://www.{clean_name}.com"
    
    print(f"DEBUG: Generierte Basis-Domain für {company_name}: {base_domain}")

    # --- 2. Crawling-Logik ---
    
    # Definiere die primären Seitenpfade, die Text enthalten sollen
    target_paths = [
        "/about", 
        "/company", 
        "/newsroom", 
        "/press", 
        "/blog"
    ]
    
    target_urls = [f"{base_domain}{path}" for path in target_paths if not base_domain.endswith('/')]
    # Füge die Hauptseite hinzu
    target_urls.append(base_domain) 

    extracted_texts = []
    
    with sync_playwright() as p:
        # Startet den Browser im Headless-Modus
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for url in target_urls:
            try:
                print(f"   Versuche, Text von {url} zu extrahieren...")
                
                # Gehe zur Seite und warte auf den Abschluss der Netzwerkaktivität
                # networkidle ist oft robuster, da es auf das Laden aller Ressourcen wartet
                response = page.goto(url, wait_until="networkidle", timeout=30000) 
                
                # Check, ob die Seite überhaupt existiert (HTTP-Statuscode 200)
                if response and response.status < 400: 
                    
                    html_content = page.content()
                    main_text = trafilatura.extract(html_content, include_comments=False, favor_precision=True)

                    if main_text and len(main_text) > 100:
                        extracted_texts.append({
                            'Ticker': ticker,
                            'Company': company_name,
                            'Source_URL': url,
                            'Content_Type': 'About/News/Blog',
                            'Raw_Text': main_text
                        })
                        print(f"   [SUCCESS] {len(main_text)} Zeichen extrahiert von {url}.")
                else:
                    print(f"   [SKIP] {url} gab einen Fehler-Statuscode zurück.")
                
            except Exception as e:
                # ERR_NAME_NOT_RESOLVED oder Timeout
                print(f"   [FAIL] Fehler bei {url}: {e}")
            
            time.sleep(1) # Throttling
        
        browser.close()
        return extracted_texts
# Beispiel-Aufruf:
all_texts = crawl_multi_target_data("Apple Inc.", "AAPL")
print(all_texts[0]['Raw_Text'][:500]) # Zeige die ersten 500 Zeichen des ersten Texts
