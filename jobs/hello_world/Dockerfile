# Usa uma imagem oficial do Python
FROM python:3.11-slim

ENV PYTHONPATH=/app

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos para o contêiner
COPY ../../jobs /app/jobs

# Instala as dependências compartilhadas
RUN pip install --no-cache-dir -r jobs/shared/requirements.txt

WORKDIR /app/jobs/hello_world

# Instala as dependências do job, se existir requirements.txt
RUN pip install --no-cache-dir -r requirements.txt || true


CMD ["python", "main.py"]