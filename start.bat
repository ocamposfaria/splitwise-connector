REM Script para executar app no Windows

@echo off

REM Ativar o venv
call venv\Scripts\activate

REM Executar FastAPI, DBT CLI e Streamlit em segundo plano
start /B uvicorn main:app --reload
start /B streamlit run streamlit/Inputs.py

REM Abrir URLs no navegador
start "" "http://127.0.0.1:8000/docs"
start "" "https://splitwise.com/"

REM Aguarda a finalização (opcional)
pause
