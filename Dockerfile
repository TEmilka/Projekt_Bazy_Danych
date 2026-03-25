FROM python:3.11-slim

WORKDIR /app

# kopiujemy requirements
COPY requirements.txt .

# instalacja bibliotek
RUN pip install --no-cache-dir -r requirements.txt

# kopiujemy kod aplikacji
COPY app/ .

CMD ["python", "main.py"]