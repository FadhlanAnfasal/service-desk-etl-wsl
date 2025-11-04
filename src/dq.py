import pandas as pd

def dq_checks(tables: dict[str, pd.DataFrame]) -> list[str]:
    issues = []

    # Ambil tabel utama
    f = tables.get("fact_reviews", pd.DataFrame())

    if not f.empty:
        # Cek review_id
        if f["review_id"].isna().any():
            issues.append("Ada review_id yang kosong di fact_reviews")
        if f["review_id"].duplicated().any():
            issues.append("Ada review_id duplikat di fact_reviews")

        # Cek rating (harus antara 1-5)
        if "rating" in f.columns:
            invalid_ratings = f[~f["rating"].between(1, 5)]
            if not invalid_ratings.empty:
                issues.append(f"Ada rating di luar rentang 1-5 ({len(invalid_ratings)} baris)")

        # Cek sentiment (harus salah satu dari tiga)
        if "sentiment" in f.columns:
            valid_sentiments = ["positive", "neutral", "negative"]
            invalid_sentiments = f[~f["sentiment"].isin(valid_sentiments)]
            if not invalid_sentiments.empty:
                issues.append(f"Ada sentiment tidak valid ({len(invalid_sentiments)} baris)")

        # Cek review_text kosong
        if f["review_text"].isna().any() or (f["review_text"].str.strip() == "").any():
            issues.append("Ada review_text kosong di fact_reviews")

        # Cek username kosong
        if f["username"].isna().any():
            issues.append("Ada username kosong di fact_reviews")

        # Cek tanggal
        if "date" in f.columns and f["date"].isna().any():
            issues.append("Ada tanggal kosong di fact_reviews")

    else:
        issues.append("Tabel fact_reviews kosong atau tidak ditemukan")

    return issues


def check_threshold(table, prev_count, threshold_ratio=0.5):
    """
    Mengecek apakah jumlah data drop signifikan dibanding run sebelumnya.
    """
    current_count = len(table)
    if prev_count > 0 and current_count < prev_count * threshold_ratio:
        return (f"Jumlah data turun drastis dari {prev_count} → {current_count} "
                f"({100 - (current_count / prev_count) * 100:.1f}% drop)")
    return None