import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd
from .config import DB_URL
from .logger import get_logger

log = get_logger(__name__)

def get_engine() -> Engine:
    return create_engine(
        DB_URL + "?options=-csearch_path=dw",
        future=True
    )

def upsert_frame(df: pd.DataFrame, table: str, key_cols: list[str]):
    engine = get_engine()
    with engine.begin() as conn:
        # pastikan schema dw aktif
        conn.execute(text("SET search_path TO dw;"))
        
        tmp = f"tmp_{table}"
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        df.head(0).to_sql(tmp, conn, schema="dw", if_exists="replace", index=False)
        df.to_sql(tmp, conn, schema="dw", if_exists="append", index=False)

        all_cols = list(df.columns)
        set_cols = [c for c in all_cols if c not in key_cols]
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols]) or ''
        keys = ", ".join(key_cols)

        merge_sql = (
            f"INSERT INTO fact_reviews (" + ", ".join(all_cols) + ")\n"
            f"SELECT " + ", ".join(all_cols) + f" FROM {tmp}\n"
            f"ON CONFLICT (" + keys + ") DO UPDATE SET " + set_clause + ";"
        )
        if set_clause.strip() == '':
            merge_sql = (
                f"INSERT INTO fact_reviews (" + ", ".join(all_cols) + ")\n"
                f"SELECT " + ", ".join(all_cols) + f" FROM {tmp}\n"
                "ON CONFLICT DO NOTHING;"
            )

        conn.execute(text(merge_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        log.info(f"✅ Upserted {len(df)} rows into dw.{table}")

if __name__ == "_main_":
    from .transform import transform_to_clean_data
    from .extract import fetch_source_data

    # Ambil data mentah dari API MRTJ
    raw_data = fetch_source_data()

    # Transformasi data ke bentuk DataFrame tunggal (reviews)
    dw_struct = transform_to_clean_data(raw_data)

    # Asumsikan transform.py return dict seperti: {"fact_reviews": df_reviews}
    for table_name, df in dw_struct.items():
        # Primary key-nya cukup review_id
        key_cols = ["review_id"]
        upsert_frame(df, table_name, key_cols)