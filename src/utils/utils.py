# src/utils.py
import socket
import time
import logging

def wait_for_broker(host: str, port: int, timeout: int = 2) -> None:
    """
    Attende che il broker sia disponibile sulla porta specificata.
    
    Parameters:
        host (str): Nome o indirizzo del broker.
        port (int): Porta del broker.
        timeout (int): Tempo di attesa tra i tentativi in secondi.
    """
    while True:
        try:
            with socket.create_connection((host, port), timeout):
                logging.info(f"Broker {host}:{port} disponibile")
                return
        except Exception as e:
            logging.info(f"Attendo broker {host}:{port}... {e}")
            time.sleep(timeout)
