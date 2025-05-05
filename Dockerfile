# Use uma imagem oficial do Python
FROM python:3.11-slim

# Cria diretório de trabalho
WORKDIR /app

# Copia os arquivos
COPY . /app

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Permite execução do script
RUN chmod +x start.sh

# Exponha as portas (8000 = FastAPI, 8501 = Streamlit)
EXPOSE 8000
EXPOSE 8501

# Comando para rodar os dois
CMD ["./start.sh"]
