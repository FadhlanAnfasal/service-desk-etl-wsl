import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from .logger import get_logger
from .config import SOURCE_API_BASE, SOURCE_API_ENDPOINT

log = get_logger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def fetch_source_data() -> list[dict]:
    url = f"{SOURCE_API_BASE}{SOURCE_API_ENDPOINT}"
    log.info(f"Requesting source API: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    log.info(f"Fetched {len(data)} records")
    if len(data) < 10:
        log.warning(f" Data volume too low: only {len(data)} records fetched")
    return data

if __name__ == "__main__":
    data = fetch_source_data()
    print(data[:3])