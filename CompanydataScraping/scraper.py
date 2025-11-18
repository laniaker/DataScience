import os
import json
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import pandas as pd
import trafilatura
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ==============================================================================
# 1. KONSTANTEN UND KONFIGURATION
# ==============================================================================

# Maximale Anzahl von Unterseiten, die pro Firma gecrawlt werden sollen
MAX_PAGES_PER_COMPANY = 25

# Verzeichnis, in dem die JSON-Dateien mit den extrahierten Daten gespeichert werden
OUTPUT_DIR = "crawled_company_data"

# Schlüsselwörter zur Identifizierung relevanter Unterseiten
RELEVANT_KEYWORDS = [
    # Deutsch
    'über-uns', 'unternehmen', 'mission', 'geschichte', 'profil',
    'nachrichten', 'presse', 'aktuelles', 'blog',
    'produkte', 'lösungen', 'dienstleistungen', 'plattform', 'technologie',
    # Englisch
    'about', 'company', 'who-we-are', 'mission', 'history', 'profile',
    'news', 'press', 'media', 'blog', 'stories', 'insights',
    'product', 'solution', 'service', 'platform', 'technology'
]

# URL-Pfade, die ignoriert werden sollen, um das Crawling auf relevante Bereiche zu beschränken
URL_BLOCKLIST = [
    # Login, Karriere & Rechtliches
    '/signin', '/login', '/register', '/signup', '/careers', '/jobs',
    'policies', 'privacy', 'terms', 'legal',
    # Spezifische, unergiebige Pfade
    '/rooms/', '/products/', '/listings/', '/experiences/', '/stays/',
    'google.com/maps', 'help.airbnb.com'
]

# Pfad zur Eingabedatei
INPUT_CSV_FILE = '/Users/lania/VSCode/DataScience/WikiNasdaq_100_constituents.csv'

# ==============================================================================
# 2. HILFSFUNKTIONEN
# ==============================================================================

def save_data_to_json(data_list, ticker, company_name):
    """Speichert die Liste der extrahierten Texte in einer unternehmensspezifischen JSON-Datei."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Bereinigt den Firmennamen für einen gültigen Dateinamen
    safe_company_name = re.sub(r'[^\w-]', '_', company_name)
    filename = f"{ticker}_{safe_company_name}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, ensure_ascii=False, indent=4)
        print(f"\n[SUCCESS] Daten für {company_name} erfolgreich gespeichert in: {filepath}")
    except IOError as e:
        print(f"\n[ERROR] Fehler beim Speichern der Datei für {company_name}: {e}")

def get_fallback_domain(company_name):
    """Generiert eine wahrscheinliche Domain aus dem Firmennamen, falls keine vorhanden ist."""
    print(f"[INFO] Keine URL in CSV gefunden. Versuche, Domain aus '{company_name}' abzuleiten...")
    
    # Spezialfälle für bekannte Unternehmen
    special_cases = {
        "alphabet": "https://abc.xyz",
        "meta platforms": "https://www.meta.com",
    }
    
    # Bereinigung des Namens
    base_name = company_name.lower()
    base_name = base_name.split('(')[0].strip()
    base_name = re.sub(r'[\s.,](inc|co|corp|ltd)\.?$', '', base_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r'[\s.\']', '', base_name)

    if clean_name in special_cases:
        domain = special_cases[clean_name]
    else:
        domain = f"https://www.{clean_name}.com"
        
    print(f"   -> Generierte Fallback-Domain: {domain}")
    return domain

def handle_cookie_banner(page):
    """Versucht, gängige Cookie-Banner zu finden und zu akzeptieren."""
    # Liste von Selektoren und Texten, die auf Cookie-Buttons hindeuten
    # XPath wird verwendet, um eine case-insensitive Suche nach Text zu ermöglichen
    cookie_selectors = [
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'akzeptieren')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'zustimmen')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'verstanden')]",
        "//button[@id='onetrust-accept-btn-handler']", # Häufig verwendete ID
    ]

    for selector in cookie_selectors:
        try:
            # Finde den ersten sichtbaren Button, der dem Selector entspricht
            button = page.locator(selector).first
            if button.is_visible(timeout=1000):
                button.click(timeout=2000)
                print("   [INFO] Cookie-Banner akzeptiert.")
                page.wait_for_timeout(1500)  # Kurze Pause, damit die Seite nach dem Klick neu laden kann
                return # Beenden, da der Banner behandelt wurde
        except Exception:
            # Button nicht gefunden oder nicht klickbar, einfach weitermachen
            continue

# ==============================================================================
# 3. KERN-CRAWLER-FUNKTION
# ==============================================================================

def crawl_company_website(company_name, ticker, base_domain):
    """
    Crawlt eine Unternehmenswebsite, extrahiert Texte von relevanten Unterseiten
    und gibt eine Liste mit den extrahierten Daten zurück.
    """
    if not base_domain or not base_domain.startswith('http'):
        print(f"[WARN] Ungültige oder fehlende Basis-Domain für {company_name}: '{base_domain}'. Überspringe.")
        return []

    print(f"[INFO] Starte Crawling für {company_name} mit Basis-Domain: {base_domain}")

    extracted_texts = []
    urls_to_visit = deque([base_domain])
    visited_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--disable-http2'])
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        page = context.new_page()

        while urls_to_visit and len(visited_urls) < MAX_PAGES_PER_COMPANY:
            current_url = urls_to_visit.popleft()
            if current_url in visited_urls:
                continue

            visited_urls.add(current_url)

            try:
                print(f"   -> Versuche: {current_url}")
                response = page.goto(current_url, wait_until="domcontentloaded", timeout=20000)

                if not response or response.status >= 400:
                    print(f"   [SKIP] Seite nicht erreichbar (Status: {response.status if response else 'N/A'}).")
                    continue

                # NEU: Versuche, einen Cookie-Banner zu behandeln
                handle_cookie_banner(page)

                page.wait_for_timeout(2000)  # Zeit für JS-Rendering geben
                html_content = page.content()
                main_text = trafilatura.extract(html_content, include_comments=False, favor_precision=True)

                if main_text and len(main_text) > 150:
                    extracted_texts.append({
                        'Ticker': ticker,
                        'Company': company_name,
                        'Source_URL': current_url,
                        'Content_Type': 'Website Content',
                        'Raw_Text': main_text
                    })
                    print(f"   [OK] {len(main_text)} Zeichen extrahiert.")

                    # Neue, relevante Links auf der aktuellen Seite finden
                    all_links = page.locator('a[href]').all()
                    for link in all_links:
                        href = link.get_attribute('href')
                        if not href:
                            continue
                        
                        link_text = link.inner_text().lower()
                        absolute_url = urljoin(current_url, href.strip())
                        
                        # Bereinige die URL von Fragmenten (#) und Query-Parametern (?)
                        parsed_url = urlparse(absolute_url)
                        clean_url = parsed_url._replace(query="", fragment="").geturl()

                        # 1. Prüfen, ob der Link zur selben Domain gehört
                        if parsed_url.netloc == urlparse(base_domain).netloc:
                            # 2. Prüfen, ob der Link auf der Blockliste steht
                            if any(blocked_path in clean_url.lower() for blocked_path in URL_BLOCKLIST):
                                continue

                            # Relevanzprüfung: Schlüsselwort in URL ODER im sichtbaren Link-Text
                            # Wir nutzen Regex für eine genauere Wort-Prüfung
                            is_relevant_by_url = any(re.search(r'\b' + re.escape(keyword) + r'\b', clean_url.lower().replace('-', ' ')) for keyword in RELEVANT_KEYWORDS)
                            is_relevant_by_text = any(re.search(r'\b' + re.escape(keyword) + r'\b', link_text) for keyword in RELEVANT_KEYWORDS)

                            if is_relevant_by_url or is_relevant_by_text:
                                if clean_url not in visited_urls and clean_url not in urls_to_visit:
                                    urls_to_visit.append(clean_url)

            except PlaywrightTimeoutError:
                print(f"   [FAIL] Timeout beim Laden von {current_url}.")
            except Exception as e:
                print(f"   [FAIL] Unerwarteter Fehler bei {current_url}: {e}")

            time.sleep(1)  # Kurze Pause zwischen den Anfragen

        browser.close()
        return extracted_texts

# ==============================================================================
# 4. HAUPT-ORCHESTRIERUNGSFUNKTION
# ==============================================================================

def run_full_crawler(csv_file, num_companies_to_test=None):
    """Liest die CSV-Datei, iteriert über die Unternehmen und startet den Crawler."""
    try:
        companies_df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"[ERROR] Die Datei {csv_file} wurde nicht gefunden.")
        return

    # Optional: Nur eine bestimmte Anzahl von Firmen zum Testen verarbeiten
    if num_companies_to_test:
        companies_df = companies_df.head(num_companies_to_test)

    for index, row in companies_df.iterrows():
        company = row['Company']
        ticker = row['Ticker']
        base_domain = row.get('Website')

        # Spezifische Start-URLs für problematische Seiten, um direkt relevantere Bereiche anzusteuern
        override_domains = {
            "AMZN": "https://www.aboutamazon.com/news",
            "NFLX": "https://about.netflix.com/en/news",
            "ABNB": "https://news.airbnb.com/"
        }
        if ticker in override_domains:
            base_domain = override_domains[ticker]

        print(f"\n{'='*60}\nBearbeite: {company} ({ticker})\n{'='*60}")

        # Fallback, falls keine URL in der CSV vorhanden oder ungültig ist
        if pd.isna(base_domain) or not isinstance(base_domain, str) or not base_domain.startswith('http'):
            base_domain = get_fallback_domain(company)

        extracted_data = crawl_company_website(company, ticker, base_domain)

        if extracted_data:
            save_data_to_json(extracted_data, ticker, company)
        else:
            print(f"[INFO] Keine relevanten Texte für {company} gefunden oder alle Versuche fehlgeschlagen.")

        print(f"Warte 5 Sekunden vor dem nächsten Unternehmen...")
        time.sleep(5)

# ==============================================================================
# 5. AUSFÜHRUNGSPUNKT
# ==============================================================================

if __name__ == "__main__":
    # Starte den Crawler. Für einen Testlauf nur die ersten 5 Firmen nehmen.
    # Für den vollen Durchlauf `num_companies_to_test=None` setzen oder den Parameter weglassen.
    run_full_crawler(INPUT_CSV_FILE)
