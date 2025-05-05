#!/bin/bash

# Roda FastAPI em background (porta 8000)
uvicorn main:app --host 0.0.0.0 --port 8000 &

# Roda Streamlit (porta 8501)
streamlit run myapp.py --server.address=0.0.0.0 --server.port=8501
