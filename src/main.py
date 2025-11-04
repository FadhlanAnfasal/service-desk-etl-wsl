from .extract import fetch_source_data
from .transform import transform_to_clean_data
from .dq import dq_checks
from .load import upsert_frame
from .logger import get_logger
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import traceback

log = get_logger(__name__)
load_dotenv()


def run(tag=None):
    start_time = datetime.now()
    log.info(f"===== Starting ETL Run ({tag}) =====")
    dq_issues_str = ""
    rows_loaded = 0
    status = "SUCCESS"

    # === DB Config ===
    DB_USER = os.getenv("DB_USER", "etl_user")
    DB_PASS = os.getenv("DB_PASS", "etl_pass")
    DB_NAME = os.getenv("DB_NAME", "servicedesk_dw")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path=dw"
    )

    try:
        # === 1. Extract ===
        raw = fetch_source_data()
        log.info(f"Fetched {len(raw)} raw review records from MRTJ API")

        # === 2. Transform ===
        df_reviews = transform_to_clean_data(raw)
        log.info(f"Transformed {len(df_reviews)} cleaned review records")

        # === 3. Data Quality Checks (optional) ===
        dq_issues = dq_checks({"fact_reviews":df_reviews})
        dq_issues_str = ", ".join(dq_issues) if dq_issues else "None"
        log.info(f"DQ Checks: {dq_issues_str}")

        # === 4. Load to PostgreSQL ===
        upsert_frame(df_reviews, "fact_reviews", ["review_id"])
        rows_loaded = len(df_reviews)
        log.info(f"✅ Loaded {rows_loaded} reviews into dw.fact_reviews")

    except Exception:
        status = "FAILED"
        dq_issues_str = traceback.format_exc()
        log.exception("ETL Failed")

    finally:
        with engine.begin() as conn:
            conn.execute(text("SET search_path TO dw;"))
            conn.execute(text("""
                INSERT INTO etl_run_log (started_at, finished_at, rows_loaded, dq_issues, status)
                VALUES (:start, now(), :rows, :issues, :status)
            """), {
                "start": start_time,
                "rows": rows_loaded,
                "issues": dq_issues_str,
                "status": status
            })
        log.info(f"ETL run complete with status={status}")


if __name__ == "__main__":
    tag = "[AUTO-CRON]" if os.getenv("CRON_RUN") else "MANUAL"
    run(tag)