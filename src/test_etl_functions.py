from .extract import fetch_source_data

def test_fetch_source_data():
    data = fetch_source_data()
    assert data is not None
    assert isinstance(data, list)
    assert len(data) > 0