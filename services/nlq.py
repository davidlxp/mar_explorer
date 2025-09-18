from services.db import get_con

def handle_question(q: str):
    con = get_con()

    # Simple heuristic routing
    if "adv" in q.lower():
        res = con.execute("SELECT month, SUM(value) FROM adv_union GROUP BY month ORDER BY month DESC LIMIT 1").fetchall()
        if res:
            return f"Latest ADV: {res[0][1]} (Month: {res[0][0]})", "ADV-M", 0.9
    elif "volume" in q.lower():
        res = con.execute("SELECT month, SUM(value) FROM volume_union GROUP BY month ORDER BY month DESC LIMIT 1").fetchall()
        if res:
            return f"Latest Volume: {res[0][1]} (Month: {res[0][0]})", "Volume-M", 0.9

    # TODO: add PR retrieval logic
    return None, None, 0.2
