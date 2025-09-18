import pandas as pd
import duckdb
from pathlib import Path
from services.db import get_con

def handle_excel_upload(file):
    df_adv = pd.read_excel(file, sheet_name="ADV-M")
    df_vol = pd.read_excel(file, sheet_name="Volume-M")

    month = "2025-08"  # TODO: extract dynamically

    out_dir = Path("storage/snapshots") / month
    out_dir.mkdir(parents=True, exist_ok=True)
    df_adv.to_parquet(out_dir / "adv.parquet", index=False)
    df_vol.to_parquet(out_dir / "volume.parquet", index=False)

    con = get_con()
    con.execute("CREATE OR REPLACE VIEW adv_union AS SELECT * FROM read_parquet('storage/snapshots/*/adv.parquet');")
    con.execute("CREATE OR REPLACE VIEW volume_union AS SELECT * FROM read_parquet('storage/snapshots/*/volume.parquet');")

    return month
