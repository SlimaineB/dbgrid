# Frontend Dockerfile

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Exposer le port FastAPI par défaut
EXPOSE 8501


# Commande pour lancer Streamlit (modifie si ton fichier a un autre nom)
CMD ["streamlit", "run", "frontend.py", "--server.port=8501", "--server.address=0.0.0.0"]