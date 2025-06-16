#!/usr/bin/env python3
import os
import asyncio
import logging
import random
import ssl
import json
from datetime import datetime, timezone

import httpx
from aiokafka import AIOKafkaProducer
from clickhouse_driver import Client as CHClient

from src.utils.logger_config import setup_logging
from src.configg import (
    KAFKA_BROKER, KAFKA_TOPIC,
    SSL_CAFILE, SSL_CERTFILE, SSL_KEYFILE,
    OSRM_URL,
    MILANO_MIN_LAT, MILANO_MAX_LAT,
    MILANO_MIN_LON, MILANO_MAX_LON,
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT, CLICKHOUSE_DATABASE,
)
from src.utils.utils import wait_for_broker

logger = logging.getLogger(__name__)
setup_logging()

async def wait_for_osrm(interval: int = 20, max_retries: int = 500):
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{OSRM_URL}/route/v1/bicycle/0,0;0,0", timeout=5)
            if r.status_code < 500:
                logger.info("OSRM è pronto")
                return
        except Exception:
            pass
        logger.debug("OSRM non pronto (tentativo %d/%d)", attempt+1, max_retries)
        await asyncio.sleep(interval)
    raise RuntimeError("OSRM non pronto dopo troppe prove")

async def wait_for_clickhouse():
    for attempt in range(30):
        try:
            client = CHClient(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            client.execute("SELECT 1")
            logger.info("ClickHouse è pronto")
            return
        except Exception:
            logger.debug("ClickHouse non pronto (tentativo %d/30)", attempt+1)
            await asyncio.sleep(2)
    raise RuntimeError("ClickHouse non pronto")

async def wait_for_kafka():
    host, port = KAFKA_BROKER.split(":")
    await asyncio.get_event_loop().run_in_executor(None, wait_for_broker, host, int(port))
    logger.info("Kafka è pronto")

def random_point_in_bbox():
    lat = random.uniform(MILANO_MIN_LAT, MILANO_MAX_LAT)
    lon = random.uniform(MILANO_MIN_LON, MILANO_MAX_LON)
    return lon, lat

async def fetch_route(start: str, end: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{OSRM_URL}/route/v1/bicycle/{start};{end}",
            params={"overview": "full", "geometries": "geojson"},
            timeout=10
        )
    r.raise_for_status()
    coords = r.json()["routes"][0]["geometry"]["coordinates"]
    return [{"lon": lon, "lat": lat} for lon, lat in coords]

async def producer_worker(producer: AIOKafkaProducer, user: tuple[int,int,str,str]):
    uid, age, profession, interests = user

    while True:
        lon1, lat1 = random_point_in_bbox()
        lon2, lat2 = random_point_in_bbox()

        try:
            route = await fetch_route(f"{lon1},{lat1}", f"{lon2},{lat2}")
        except Exception as e:
            logger.error("Utente %d: impossibile fetchare percorso: %s", uid, e)
            await asyncio.sleep(5)
            continue

        for pt in route:
            msg = {
                "user_id":     uid,
                "latitude":    pt["lat"],
                "longitude":   pt["lon"],
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "age":         age,
                "profession":  profession,
                "interests":   interests,
            }
            try:
                # msg viene serializzato in JSON bytes grazie al value_serializer
                await producer.send_and_wait(KAFKA_TOPIC, value=msg)
                logger.debug("Utente %d → inviato punto %s", uid, msg)
            except Exception as e:
                logger.error("Utente %d: errore invio Kafka: %s", uid, e)
            await asyncio.sleep(2)

async def main():
    # readiness checks
    await asyncio.gather(
        wait_for_kafka(),
        wait_for_clickhouse(),
        wait_for_osrm()
    )

    # prepara SSLContext
    ssl_ctx = ssl.create_default_context(cafile=SSL_CAFILE)
    ssl_ctx.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)

    # inizializza producer con JSON serializer
    producer = AIOKafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        security_protocol="SSL",
        ssl_context=ssl_ctx,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        # carica utenti
        ch = CHClient(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        users = ch.execute("SELECT user_id, age, profession, interests FROM users")
        if not users:
            logger.error("Nessun utente trovato in ClickHouse")
            return

        # un worker per utente
        tasks = [producer_worker(producer, u) for u in users]
        await asyncio.gather(*tasks)

    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())