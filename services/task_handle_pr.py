from sentence_transformers import SentenceTransformer
from services.db import get_con

def handle_pr_upload(file):
    text = file.read().decode("utf-8", errors="ignore")
    chunks = [p.strip() for p in text.split("\n") if p.strip()]

    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(chunks)

    con = get_con()
    con.execute("DELETE FROM pr_index;")
    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        con.execute("INSERT INTO pr_index VALUES (?, ?, ?)", [i, chunk, emb.tolist()])
