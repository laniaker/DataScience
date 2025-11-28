from sentence_transformers import SentenceTransformer
import csv
import json


model = SentenceTransformer('all-MiniLM-L6-v2')

def load_data():
   news = []
   with open("DataScience/Embedding/data/output.csv", newline='', encoding='utf-8') as csvfile:
      reader = csv.DictReader(csvfile)
      for row in reader:
         news.append({
            "name": row["Company"],
            "ticker": row["Ticker"],
            "raw": row["Raw_Text"]
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

texts = [f"{company["raw"]}" for company in news]

embeddings = get_embedding(texts)

# Embeddings den News zuordnen
for i, company in enumerate(news):
    if hasattr(embeddings[i], "tolist"):
        company["embedding"] = embeddings[i].tolist()
    else:
        company["embedding"] = embeddings[i]

fields = ["name","ticker","text","embedding"]

csv_file_path = 'DataScience/Embeddding/data/nasdaq100_embeddingWeb.csv'

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    for company in news:
        row = {
            "name": company["name"],
            "ticker": company["ticker"],
            "text": company["raw"],
            "embedding": json.dumps(company["embedding"])
        }
        writer.writerow(row)

print(f"CSV file '{csv_file_path}' created successfully.")
