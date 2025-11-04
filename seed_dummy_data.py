import requests
import random
import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# 🔧 DB Config
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASS = os.getenv("DB_PASS", "etl_pass")
DB_NAME = os.getenv("DB_NAME", "servicedesk_dw")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 🌐 Fetch dummy posts
response = requests.get("https://jsonplaceholder.typicode.com/posts")
posts = response.json()

# 🎲 Generate randomized ticket data
tickets = []
statuses = ["open", "in_progress", "resolved", "closed"]
priorities = ["low", "medium", "high"]

start_date = datetime.datetime(2025, 1, 1)
end_date = datetime.datetime(2025, 1, 31)
delta = (end_date - start_date).days 

for post in posts[:50]:  # ambil 50 data aja biar ga berat
    created_time = start_date + datetime.timedelta(days=random.randint(0, delta))
    resolution_minutes = random.randint(30, 600)
    resolved_time = created_time + datetime.timedelta(minutes=resolution_minutes)
    is_sla_breached = random.choice([True, False])
    
    tickets.append({
        "ticket_id": post["id"],
        "user_id": post["userId"],
        "status": random.choice(statuses),
        "priority": random.choice(priorities),
        "created_time": created_time,
        "resolved_time": resolved_time,
        "resolution_time_min": resolution_minutes,
        "is_sla_breached": is_sla_breached
    })

# 🧠 Insert into DB
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dw.fact_ticket RESTART IDENTITY CASCADE;"))  # bersihin dulu
    for t in tickets:
        conn.execute(text("""
            INSERT INTO dw.fact_ticket 
            (ticket_id, user_id, status, priority, created_time, resolved_time, resolution_time_min, is_sla_breached)
            VALUES (:ticket_id, :user_id, :status, :priority, :created_time, :resolved_time, :resolution_time_min, :is_sla_breached)
        """), t)

print(f"✅ Inserted {len(tickets)} dummy tickets successfully!")