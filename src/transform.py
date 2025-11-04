import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .logger import get_logger
from .extract import fetch_source_data

log = get_logger(__name__)

def transform_to_clean_data(raw: list[dict]) -> pd.DataFrame:
    """
    Transformasi data review MRT Jakarta API ke format yang lebih rapi
    """

    # Convert ke DataFrame
    df = pd.DataFrame(raw)

    # Rename biar seragam (hapus spasi di nama kolom)
    df = df.rename(columns={
        "Review Id": "review_id",
        "Username": "username",
        "Rating": "rating",
        "Review Text": "review_text",
        "Date": "date"
    })

    # Bersihin data (hapus duplikat, ubah tipe data)
    df = df.drop_duplicates(subset="review_id")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Tambahin kolom tambahan (opsional)
    df["sentiment"] = df["rating"].apply(
        lambda x: "positive" if x >= 4 else ("negative" if x <= 2 else "neutral")
    )

    log.info(f"✅ Cleaned {len(df)} review records from MRTJ API")
    return df


if __name__ == "__main__":
    # Fetch data langsung dari API Railway lu
    raw_data = fetch_source_data()

    # Transform jadi clean dataframe
    clean_df = transform_to_clean_data(raw_data)

    # Simpen hasilnya ke file CSV biar bisa dipakai ETL selanjutnya
    output_path = os.path.join(os.path.dirname(__file__), "../data/clean_mrtj_reviews.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    clean_df.to_csv(output_path, index=False)

    print("\n===== CLEANED DATA SAMPLE =====")
    print(clean_df.head())
    print(f"\n✅ File saved to: {output_path}")
