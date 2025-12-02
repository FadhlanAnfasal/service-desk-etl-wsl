from unittest.mock import MagicMock, patch

import pandas as pd

from src.load import upsert_frame
from src.transform import transform_to_clean_data

from .extract import fetch_source_data


def test_fetch_source_data():
    data = fetch_source_data()
    assert data is not None
    assert isinstance(data, list)
    assert len(data) > 0


def test_transform_to_clean_data():
    raw = [
        {
            "Review Id": "1",
            "Username": "alice",
            "Rating": "5",
            "Review Text": "Nice",
            "Date": "2024-01-01",
        }
    ]

    df = transform_to_clean_data(raw)

    assert df is not None
    assert len(df) == 1
    assert "sentiment" in df.columns
    assert df.loc[0, "sentiment"] == "positive"


@patch("src.load.get_engine")
def test_upsert_frame(mock_engine):
    # Mock engine + connection
    mock_conn = MagicMock()
    mock_engine.return_value.begin.return_value.__enter__.return_value = (
        mock_conn
    )

    df = pd.DataFrame({"review_id": ["1"], "rating": [5], "username": ["a"]})

    upsert_frame(df, "fact_reviews", ["review_id"])

    assert mock_engine.called
    assert mock_conn.execute.called
