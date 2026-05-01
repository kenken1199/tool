@echo off
cd /d "%~dp0"
call C:\Users\k-nakamori\tool\.venv\Scripts\activate.bat
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
pause
