import pandas as pd
from src.transform import transform_to_dw_struct
from src.dq import dq_checks

def test_transform_output_structure():
    mock_data = [{"userId": 1, "id": 1, "title": "Sample title", "body": "desc"}]
    result = transform_to_dw_struct(mock_data)
    assert "fact_ticket" in result
    assert "dim_user" in result
    cols = result["fact_ticket"].columns
    expected = ["ticket_id", "user_id", "category_id", "sla_id", "subject", "description"]
    for col in expected:
        assert col in cols

def test_dq_check_flags():
    data = {
        "dim_user": pd.DataFrame({"user_id": [1,1, None]}),
        "fact_ticket": pd.DataFrame({"ticket_id": [1,2,2]})}
    issues = dq_checks(data)
    assert "Duplicate" in str(issues) or "Missing" in str(issues) or "Missing IDs" in str(issues)