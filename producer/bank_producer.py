"""
bank_producer.py  —  Microservicio Productor
Publica mensajes bancarios en bucle cada INTERVAL segundos.
Variables de entorno:
  PUBLISH_INTERVAL  segundos entre lotes (default 10)
  RABBITMQ_HOST / PORT / VHOST / USER / PASS
"""
import json
import time
import random
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.rabbitmq_config import (
    get_connection, setup_topology,
    EXCHANGE_TOPIC, EXCHANGE_FANOUT, EXCHANGE_DIRECT,
    RK_FRAUD_HIGH, RK_FRAUD_LOW
)
import pika

logger = logging.getLogger('Producer')
INTERVAL = int(os.getenv('PUBLISH_INTERVAL', 10))


# ── Generadores de mensajes ──────────────────────────────────────────────────

def make_transfer():
    accounts = ['ACC-001', 'ACC-002', 'ACC-003', 'ACC-004']
    return {
        'type': 'TRANSFER',
        'transaction_id': f"TXN-{random.randint(100000, 999999)}",
        'from_account': random.choice(accounts),
        'to_account':   random.choice(accounts),
        'amount':   round(random.uniform(10, 5000), 2),
        'currency': 'USD',
        'timestamp': datetime.now().isoformat()
    }


def make_payment():
    merchants = ['Amazon', 'Netflix', 'Spotify', 'Uber', 'Apple', 'Google']
    return {
        'type': 'PAYMENT',
        'transaction_id': f"PAY-{random.randint(100000, 999999)}",
        'account':  f"ACC-{random.randint(1, 4):03d}",
        'merchant': random.choice(merchants),
        'amount':   round(random.uniform(1, 500), 2),
        'currency': 'USD',
        'timestamp': datetime.now().isoformat()
    }


def make_alert():
    tipos = ['MULTIPLE_FAILED_LOGINS', 'UNUSUAL_LOCATION', 'LARGE_TRANSACTION', 'NEW_DEVICE']
    return {
        'type': 'SECURITY_ALERT',
        'alert_id':   f"ALT-{random.randint(10000, 99999)}",
        'alert_type': random.choice(tipos),
        'account':    f"ACC-{random.randint(1, 4):03d}",
        'severity':   random.choice(['LOW', 'MEDIUM', 'HIGH']),
        'timestamp':  datetime.now().isoformat()
    }


def make_fraud(high=None):
    if high is None:
        high = random.random() > 0.5
    return {
        'type': 'FRAUD_DETECTION',
        'event_id': f"FRD-{random.randint(10000, 99999)}",
        'account':  f"ACC-{random.randint(1, 4):03d}",
        'score':    round(random.uniform(0.7, 1.0) if high else random.uniform(0.3, 0.69), 3),
        'high_priority': high,
        'timestamp': datetime.now().isoformat()
    }


# ── Publicación ──────────────────────────────────────────────────────────────

PROPS = pika.BasicProperties(delivery_mode=2, content_type='application/json')


def publish(channel, exchange, routing_key, payload):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=PROPS
    )


def publish_batch(channel):
    """Publica un lote de 5 mensajes (uno por tipo)."""
    # 1. Transferencia → Topic
    msg = make_transfer()
    publish(channel, EXCHANGE_TOPIC, 'txn.transfer.domestic', msg)
    logger.info(f"[TOPIC]  Transferencia ${msg['amount']} | {msg['from_account']} → {msg['to_account']}")

    # 2. Pago → Topic
    msg = make_payment()
    publish(channel, EXCHANGE_TOPIC, 'txn.payment.merchant', msg)
    logger.info(f"[TOPIC]  Pago ${msg['amount']} → {msg['merchant']}")

    # 3. Alerta → Fanout
    msg = make_alert()
    publish(channel, EXCHANGE_FANOUT, '', msg)
    logger.info(f"[FANOUT] Alerta {msg['alert_type']} | severidad={msg['severity']}")

    # 4. Fraude alta → Direct
    msg = make_fraud(high=True)
    publish(channel, EXCHANGE_DIRECT, RK_FRAUD_HIGH, msg)
    logger.info(f"[DIRECT] Fraude ALTA | score={msg['score']} | {msg['account']}")

    # 5. Fraude baja → Direct
    msg = make_fraud(high=False)
    publish(channel, EXCHANGE_DIRECT, RK_FRAUD_LOW, msg)
    logger.info(f"[DIRECT] Fraude BAJA | score={msg['score']} | {msg['account']}")


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    logger.info("=== Productor Bancario arrancando ===")
    connection = None
    while True:
        try:
            if connection is None or connection.is_closed:
                logger.info("Conectando a RabbitMQ...")
                connection = get_connection()
                channel = connection.channel()
                setup_topology(channel)
                logger.info("Conexión establecida ✓")

            publish_batch(channel)
            logger.info(f"--- Lote publicado. Próximo en {INTERVAL}s ---")
            time.sleep(INTERVAL)

        except Exception as e:
            logger.error(f"Error: {e}. Reintentando en 5s...")
            try:
                connection.close()
            except Exception:
                pass
            connection = None
            time.sleep(5)


if __name__ == '__main__':
    main()
