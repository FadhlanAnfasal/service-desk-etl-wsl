import pytest
from src.main import run 
from src.load import get_engine
from sqlalchemy import text

@pytest.mark.integration
def test_full_etl_pipeline():
    """Test the full ETL pipeline from extract -> transform -> load."""

    run()

    engine = get_engine()
    with engine.connect() as conn:
        # Check dim_user table
        result = conn.execute(text("SELECT COUNT(*) FROM dw.dim_user"))
        user_count = result.scalar()
        assert user_count > 0, "dim_user table should have rows after ETL"

        # Check dim_category table
        result = conn.execute(text("SELECT COUNT(*) FROM dw.dim_category"))
        category_count = result.scalar()
        assert category_count > 0, "dim_category table should have rows after ETL"

        # Check dim_sla table
        result = conn.execute(text("SELECT COUNT(*) FROM dw.dim_sla"))
        sla_count = result.scalar()
        assert sla_count > 0, "dim_sla table should have rows after ETL"

        # Check fact_ticket table
        result = conn.execute(text("SELECT COUNT(*) FROM dw.fact_ticket"))
        ticket_count = result.scalar()
        assert ticket_count > 0, "fact_ticket table should have rows after ETL"

