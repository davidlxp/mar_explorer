# from sentence_transformers import SentenceTransformer
from services.db import Database
import requests
from bs4 import BeautifulSoup



# def handle_pr_upload(file):
#     text = file.read().decode("utf-8", errors="ignore")
#     chunks = [p.strip() for p in text.split("\n") if p.strip()]

#     model = SentenceTransformer("all-MiniLM-L6-v2")
#     embeddings = model.encode(chunks)

#     con = get_con()
#     con.execute("DELETE FROM pr_index;")
#     for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
#         con.execute("INSERT INTO pr_index VALUES (?, ?, ?)", [i, chunk, emb.tolist()])



def fetch_press_release():

    url = "https://www.tradeweb.com/newsroom/media-center/news-releases/tradeweb-reports-august-2025-total-trading-volume-of--$54.1-trillion-and-average-daily-volume-of-$2.5-trillion"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    # print(html)

    print(soup.find_all("p"))

    # paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
    # text = "\n\n".join(paragraphs)
    # print(text)