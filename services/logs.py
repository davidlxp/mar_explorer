from services.db import get_con

def log_question(question, confidence, citations):
    con = get_con()
    con.execute("INSERT INTO logs (question, confidence, citations) VALUES (?, ?, ?)",
                [question, confidence, str(citations)])
