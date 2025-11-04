import os
from dotenv import load_dotenv
from pathlib import Path

# Cari .env berdasarkan lokasi file ini (bukan current working dir)
env_path = Path(__file__).resolve().parent.parent / ".env"

if env_path.exists():
    print(f" Loading .env from: {env_path}")
    load_dotenv(dotenv_path=env_path)
else:
    print("⚠ .env file not found. Please check your project structure.")

# Database connection URL
DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER', 'etl_user')}:"
    f"{os.getenv('DB_PASS', 'etl_pass')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'servicedesk_dw')}"
)

# API Config
SOURCE_API_BASE = os.getenv("SOURCE_API_BASE", "https://mrtj-api-production.up.railway.app")
SOURCE_API_ENDPOINT = os.getenv("SOURCE_API_ENDPOINT", "/")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

print("BASE =", SOURCE_API_BASE)
print("ENDPOINT =", SOURCE_API_ENDPOINT)