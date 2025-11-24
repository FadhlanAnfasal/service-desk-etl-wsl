# flake8: noqa
import os
import sys

sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/src")
sys.path.append(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)  # noqa: E501

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from pendulum import timezone

from src.main import run as etl_run

local_tz = timezone("Asia/Jakarta")


def notify_failure(context):
    """Kirim email otomatis kalau task gagal"""
    subject = (
        "❌ Airflow DAG Gagal -" f"{context['task_instance'].dag_id}"
    )  # noqa: E501
    body = f"""
    <h3>Task Gagal!</h3>
    <p><b>DAG:</b> {context['task_instance'].dag_id}</p>
    <p><b>Task:</b> {context['task_instance'].task_id}</p>
    <p><b>Waktu:</b> {context['execution_date']}</p>
    <p>Silahkan untuk cek log di Web Airflow untuk melihat detail lebih lanjut.</p>
    """
    send_email(
        to=["fadhlananfasalhajji1403@mail.ugm.ac.id"],
        subject=subject,
        html_content=body,
    )  # noqa: E501


default_args = {
    "owner": "fadhlan",
    "email": ["fadhlananfasalhajji1403@mail.ugm.ac.id"],
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def notify_success():
    subject = "✅ Airflow DAG Berhasil"
    body = "DAG service_desk_etl successfull 💪"
    send_email(
        to=["fadhlaanalle149@gmail.com"], subject=subject, html_content=body
    )  # noqa: E501


def send_teams_notification():
    TEAMS_WEBHOOK_URL = (
        "https://defaultaf2c0734cb42464fb6bf2a241b6ada"
        ".56.environment.api.powerplatform.com:443"
        "/powerautomate/automations/direct/workflows/"
        "03a95c53672045e19e4913e3b954649a/triggers/manual/"
        "paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&"
        "sv=1.0&sig=r-rntXvZWr8ecSPjRWKmUUzZ4FksrU2dzd-4x6u5BGg"
    )

    payload = {
        "text": (
            "✅ ETL pipeline successfull at"
            f"{datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')} 💪"
        )
    }
    headers = {"Content-Type": "application/json"}

    try:
        res = requests.post(
            TEAMS_WEBHOOK_URL, json=payload, headers=headers, timeout=15
        )
        print(f"Response status: {res.status_code}")
        print(f"Response body: {res.text}")
        res.raise_for_status()
    except Exception as e:
        print(f"Error kirim Teams: {e}")
        raise


with DAG(
    dag_id="service_desk_etl",
    description="ETL pipeline Postgres",
    schedule="@hourly",
    start_date=datetime(2025, 11, 10, tzinfo=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["servicedesk"],
) as dag:

    etl_task = PythonOperator(task_id="run_etl", python_callable=etl_run)

    success_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    teams_notify = PythonOperator(
        task_id="teams_notify", python_callable=send_teams_notification
    )

    etl_task >> success_notify >> teams_notify
