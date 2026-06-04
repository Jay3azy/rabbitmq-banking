"""
wait_for_services.py
Espera hasta que RabbitMQ y (opcionalmente) PostgreSQL estén listos antes de arrancar.
"""
import os
import sys
import time
import socket
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger('wait')


def wait_for_port(host, port, service, timeout=60):
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info(f"✓ {service} listo en {host}:{port}")
                return True
        except (ConnectionRefusedError, OSError):
            elapsed = time.time() - start
            if elapsed >= timeout:
                logger.error(f"✗ Timeout esperando {service} en {host}:{port}")
                sys.exit(1)
            logger.info(f"  Esperando {service} ({int(elapsed)}s)...")
            time.sleep(3)


if __name__ == '__main__':
    wait_for_port(
        os.getenv('RABBITMQ_HOST', 'rabbitmq'), 5672, 'RabbitMQ'
    )
    db_host = os.getenv('DB_HOST', '')
    if db_host:
        wait_for_port(db_host, 5432, 'PostgreSQL')
