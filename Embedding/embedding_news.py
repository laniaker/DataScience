from sentence_transformers import SentenceTransformer
import csv
import json


model = SentenceTransformer('all-MiniLM-L6-v2')

def load_data():
   news = []
   with open("DataScience/DataScience_Sandbox/gesammelte_nasdaq_news_doublekey.csv", newline='', encoding='utf-8') as csvfile:
      reader = csv.DictReader(csvfile)
      for row in reader:
         news.append({
            "name": row["company_name"],
            "ticker": row["ticker"],
            "title": row["title"],
            "description": row["description"]
         })
   return (news)

def get_embedding(text):
    embeddings = model.encode(
        text,
        batch_size=128,
        convert_to_numpy=True,
        show_progress_bar=True
    )    
    return embeddings 

news = load_data()

combined_texts = [f"{company['title']} [SEP] {company['description']}" for company in news]

embeddings = get_embedding(combined_texts)

# Embeddings den News zuordnen
for i, company in enumerate(news):
    if hasattr(embeddings[i], "tolist"):
        company["embedding"] = embeddings[i].tolist()
    else:
        company["embedding"] = embeddings[i]

fields = ["name","ticker","text","embedding"]

csv_file_path = 'DataScience/Embedding/data/nasdaq100_embedding.csv'

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    for company in news:
        row = {
            "name": company["name"],
            "ticker": company["ticker"],
            "text": company["title"],
            "embedding": json.dumps(company["embedding"])
        }
        writer.writerow(row)

print(f"CSV file '{csv_file_path}' created successfully.")
