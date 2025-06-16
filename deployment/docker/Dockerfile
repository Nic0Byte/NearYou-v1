# deployment/docker/Dockerfile

FROM python:3.10-slim

# Aggiorna gli indici dei pacchetti e installa il client Docker
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

# Imposta la directory di lavoro
WORKDIR /workspace

# Copia il file delle dipendenze e installa i pacchetti Python necessari
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia la cartella del progetto e gli script di inizializzazione
COPY . /workspace
COPY deployment/scripts /workspace/deployment/scripts

# Imposta i permessi di esecuzione per tutti gli script .sh in /deployment/scripts
RUN chmod +x /workspace/deployment/scripts/*.sh

# Espone src/ e workspace nel PYTHONPATH così da importare i moduli (es. configg)
ENV PYTHONPATH=/workspace/src:/workspace

# Mantieni il container in esecuzione (default)
CMD ["sleep", "infinity"]