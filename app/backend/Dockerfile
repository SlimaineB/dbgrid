# Backend Dockerfile

FROM python:3.11-slim

# Variables d'environnement pour éviter buffering Python (plus rapide logs)
ENV PYTHONUNBUFFERED=1

# Installer les dépendances système si nécessaire (ex: build-essential si besoin)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier requirements.txt (assure-toi de le créer dans le dossier frontend)
COPY requirements.txt .

# Installer les dépendances Python
RUN pip3 install --no-cache-dir -r requirements.txt

# Copier le code de l'app
COPY . .

# Exposer le port standard Streamlit (par défaut 8501)
EXPOSE 8000

# Commande pour lancer uvicorn
CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]