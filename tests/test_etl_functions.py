import pandas as pd
from src.transform import transform_to_dw_struct
from src.dq import dq_checks

def test_transform_columns():
    raw = [
        {"ticket_id" : 1,
         "userId": 1,
         "subject": "Network issue",
         "description": "Internet connection is slow",
         "status": "open",
         "priority": "high",
         "created_time": "2025-01-01T10:00:00Z",
         "resolved_time": "2025-01-01T11:00:00Z",
         },
        {"ticket_id": 2,
         "userId": 2,
         "subject": "Email problem",
         "description": "Cannot send email",
         "status": "closed",
         "priority": "low",
         "created_time": "2025-01-02T10:00:00Z",
         "resolved_time": "2025-01-02T12:00:00Z",
         }
    ]

    tables = transform_to_dw_struct(raw)
    assert set(tables.keys()) == {"dim_user", "dim_category", "dim_sla", "fact_ticket"}

def test_dq_check_flags():
    data = {
        "dim_user": pd.DataFrame({"user_id": [1, 1, None]}),
        "fact_ticket": pd.DataFrame({"ticket_id": [1, 2, 2]})
    }
    issues = dq_checks(data)
    assert "Duplicate" in str(issues) or "Missing" in str(issues)
