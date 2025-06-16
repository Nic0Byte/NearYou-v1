#!/usr/bin/env python3
import random
import time
from datetime import datetime, date
import logging

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as CHError
from faker import Faker

from src.utils.db_utils import wait_for_clickhouse_database
from src.utils.logger_config import setup_logging
from src.configg import (
    CLICKHOUSE_HOST, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_PORT,
    CLICKHOUSE_DATABASE
)

setup_logging()
logger = logging.getLogger(__name__)

NUM_USERS = 5  # quanti utenti creare

# ——————————————————————————————————————————————————————————————
# Elenchi verosimili
PROFESSIONS = [
    "Ingegnere", "Medico", "Avvocato", "Insegnante",
    "Commercialista", "Architetto", "Farmacista",
    "Giornalista", "Psicologo", "Ricercatore"
]

INTERESTS = [
    "caffè", "bicicletta", "arte", "cinema",
    "fitness", "lettura", "fotografia", "musica",
    "viaggi", "cucina", "sport", "tecnologia"
]

ITALIAN_CITIES = [
    "Milano", "Roma", "Torino", "Napoli", "Bologna",
    "Firenze", "Genova", "Venezia", "Verona", "Palermo"
]

EMAIL_DOMAINS = [
    "gmail.com", "libero.it", "hotmail.it", "yahoo.it",
    "alice.it", "tiscali.it"
]

# ——————————————————————————————————————————————————————————————
# Faker per nome/cognome e telefono
fake = Faker('it_IT')

# ClickHouse client
client = Client(
    host=CLICKHOUSE_HOST,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    port=CLICKHOUSE_PORT,
    database=CLICKHOUSE_DATABASE
)

def wait_for_table(table_name: str, timeout: int = 2, max_retries: int = 30) -> bool:
    retries = 0
    while retries < max_retries:
        try:
            tables = [t[0] for t in client.execute("SHOW TABLES")]
            if table_name in tables:
                logger.info("La tabella '%s' è disponibile.", table_name)
                return True
        except CHError as e:
            logger.error("Errore controllo tabella '%s': %s", table_name, e)
        time.sleep(timeout)
        retries += 1
    raise Exception(f"Tabella '{table_name}' non trovata dopo {max_retries} tentativi.")

def calculate_age(birthdate: date) -> int:
    today = date.today()
    return today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

def generate_user_record(user_id: int) -> tuple:
    # Nome e cognome verosimili
    full_name = fake.name()
    first, last = full_name.split(" ", 1)
    # Username: first + last lowercase
    username = (first + last).lower().replace("'", "").replace(" ", "")
    # Email basata su username e dominio casuale
    email = f"{username}@{random.choice(EMAIL_DOMAINS)}"
    # Sesso e data di nascita Faker
    profile = fake.simple_profile()
    gender = "Male" if profile["sex"] == "M" else "Female"
    birthdate = profile["birthdate"]
    age = calculate_age(birthdate)

    # Campi da elenchi definiti
    profession = random.choice(PROFESSIONS)
    interests = ", ".join(random.sample(INTERESTS, k=3))
    country = "Italia"
    city = random.choice(ITALIAN_CITIES)

    # Contatto e credenziali
    phone_number = fake.phone_number()
    password = fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True)

    # Tipo utente
    user_type = random.choice(["free", "premium"])

    # Timestamp di registrazione
    registration_time = datetime.now()

    return (
        user_id,
        username,
        full_name,
        email,
        phone_number,
        password,
        user_type,
        gender,
        age,
        profession,
        interests,
        country,
        city,
        registration_time
    )

def insert_users(num_users: int):
    logger.info("Generazione di %d utenti verosimili...", num_users)
    records = [generate_user_record(i+1) for i in range(num_users)]
    query = """
        INSERT INTO users (
            user_id, username, full_name, email, phone_number,
            password, user_type, gender, age, profession,
            interests, country, city, registration_time
        ) VALUES
    """
    try:
        client.execute(query, records)
        logger.info("Inseriti %d utenti nella tabella 'users'.", num_users)
    except CHError as e:
        logger.error("Errore inserimento utenti: %s", e)

if __name__ == '__main__':
    # 1) Attendi che il DB e la tabella siano pronti
    wait_for_clickhouse_database(client, CLICKHOUSE_DATABASE)
    wait_for_table("users")

    # 2) Inserisci i profili
    insert_users(NUM_USERS)
    logger.info("Operazione completata con successo.")