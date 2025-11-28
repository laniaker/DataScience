import pandas as pd
import numpy as np
import json
from sklearn.metrics.pairwise import cosine_similarity

# CSV einlesen
df = pd.read_csv('DataScience/Embedding/data/nasdaq100_embeddingWeb.csv')
#df = pd.read_csv('DataScience/Embedding/data/nasdaq100_embedding.csv')

# Embeddings zurück in numpy Arrays konvertieren
embeddings = np.array([json.loads(emb) for emb in df['embedding']])

# Wähle eine bestimmte News aus (z.B. die erste)
test_index = 3
test_embedding = embeddings[test_index:test_index+1]

print(f"Ausgewählte News: {df.iloc[test_index]['text']}\n")
print(f"Company: {df.iloc[test_index]['name']} ({df.iloc[test_index]['ticker']})\n")

# Ähnlichkeiten zu allen anderen berechnen
similarities = cosine_similarity(test_embedding, embeddings)[0]

# Top 6 ähnlichste finden (inkl. sich selbst)
top_indices = similarities.argsort()[-6:][::-1]

print("Top 5 ähnlichste News:\n")
for i, idx in enumerate(top_indices[1:], 1):  # [1:] um sich selbst zu überspringen
    print(f"{i}. Similarity: {similarities[idx]:.4f}")
    print(f"   Text: {df.iloc[idx]['text']}")
    print(f"   Company: {df.iloc[idx]['name']} ({df.iloc[idx]['ticker']})")
    print()

