# from sentence_transformers import SentenceTransformer
from services.db import Database
import requests
from bs4 import BeautifulSoup

EXCLUDE_HEADERS = [
    "Media Contacts",
    "Market and Industry Data",
    "Forward-Looking Statements",
]


# def handle_pr_upload(file):
#     text = file.read().decode("utf-8", errors="ignore")
#     chunks = [p.strip() for p in text.split("\n") if p.strip()]

#     model = SentenceTransformer("all-MiniLM-L6-v2")
#     embeddings = model.encode(chunks)

#     con = get_con()
#     con.execute("DELETE FROM pr_index;")
#     for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
#         con.execute("INSERT INTO pr_index VALUES (?, ?, ?)", [i, chunk, emb.tolist()])

URL = "https://www.tradeweb.com/newsroom/media-center/news-releases/tradeweb-reports-may-2025-total-trading-volume-of--$55.4-trillion-and-average-daily-volume-of-$2.5-trillion"


def fetch_press_release(url: str) -> str:

    
    


    



