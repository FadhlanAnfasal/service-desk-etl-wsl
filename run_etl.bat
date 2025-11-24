@echo off
cd /d "C:\Users\ACER\Documents\service-desk-etl"
call venv\Scripts\activate
python -m src.main >> logs\task_log.txt 2>&1
