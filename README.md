# NearYou: Sistema di Notifiche basate sulla Posizione

## Panoramica
NearYou è una piattaforma che offre notifiche personalizzate agli utenti quando si trovano in prossimità di negozi o punti di interesse. Utilizzando tecnologie di streaming dati, database geospaziali e generazione di messaggi con LLM, il sistema crea un'esperienza utente contestuale e personalizzata.

## Caratteristiche Principali
- Tracciamento posizioni in tempo reale con Kafka
- Ricerca geospaziale di negozi vicini con PostgreSQL/PostGIS
- Messaggi personalizzati generati con LLM (via Groq/OpenAI)
- Dashboard utente interattiva con visualizzazione mappa
- Monitoraggio e analisi completi con Grafana e Prometheus

## Architettura
![Architettura del Sistema](docs/architecture/diagrams/architecture_overview.png)

NearYou utilizza un'architettura a microservizi:
- **Data Pipeline**: Producer e Consumer Kafka per elaborazione eventi posizione
- **Message Generator**: Servizio di generazione messaggi personalizzati
- **Dashboard**: Interfaccia web per visualizzare notifiche e posizioni
- **Storage**: ClickHouse per analytics, PostgreSQL/PostGIS per dati geospaziali
- **Cache**: Redis per memorizzare risposte LLM e migliorare performance

[Documentazione architetturale dettagliata](docs/architecture/overview.md)

## Installazione e Setup

### Prerequisiti
- Docker e Docker Compose
- Git
- Make (opzionale)

### ETL AGG, CONSUMI: 
~4-8 run/mese (media 6) + check giornalieri:
- Check giornaliero: <1 secondo, query singola
- ETL completo: 6 × 500MB = 3GB/mese
- CPU: 5 min × 6 = 30 min/mese + 30 sec check = ~31 min/mese
- Query PostgreSQL: 300k query/mese
- Costo stimato: €0.60-1/mese

### Installazione Rapida
```bash
# Clona il repository
git clone https://github.com/yourusername/nearyou.git
cd nearyou

# Configura le variabili d'ambiente
cp .env.example .env
# Modifica .env con i tuoi valori

# Avvia i servizi
docker-compose up -d