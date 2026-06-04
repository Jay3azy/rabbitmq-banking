"""
transaction_consumer.py  —  Microservicio Consumidor 1 (Topic)
Consume transferencias y pagos, los guarda en PostgreSQL
y expone una API Flask para consultarlos.

Rutas:
  GET /health              → estado del servicio
  GET /transactions        → lista todas las transacciones
  GET /transactions/<id>   → detalle de una transacción
  GET /transactions/stats  → estadísticas (total, suma, por tipo)
"""
import json
import os
import sys
import logging
import threading
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.rabbitmq_config import get_connection, setup_topology, QUEUE_AUDIT, QUEUE_PAYMENTS

logger = logging.getLogger('TransactionConsumer')

# ── Base de datos ────────────────────────────────────────────────────────────

DB_URL = os.getenv('DATABASE_URL', 'postgresql://admin:admin123@localhost:5432/transactions_db')


def get_db():
    return psycopg2.connect(DB_URL)


def init_db():
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id            SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(50) UNIQUE NOT NULL,
                    type          VARCHAR(20) NOT NULL,
                    from_account  VARCHAR(20),
                    to_account    VARCHAR(20),
                    merchant      VARCHAR(100),
                    amount        NUMERIC(12,2) NOT NULL,
                    currency      VARCHAR(5) DEFAULT 'USD',
                    queue_source  VARCHAR(30),
                    received_at   TIMESTAMP DEFAULT NOW(),
                    raw_message   JSONB
                )
            """)
    conn.close()
    logger.info("Base de datos inicializada ✓")


def save_transaction(data, queue_source):
    conn = get_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions
                        (transaction_id, type, from_account, to_account,
                         merchant, amount, currency, queue_source, raw_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    data.get('transaction_id'),
                    data.get('type'),
                    data.get('from_account'),
                    data.get('to_account'),
                    data.get('merchant'),
                    data.get('amount'),
                    data.get('currency', 'USD'),
                    queue_source,
                    json.dumps(data)
                ))
    finally:
        conn.close()


# ── Callbacks RabbitMQ ───────────────────────────────────────────────────────

def on_transfer(ch, method, properties, body):
    data = json.loads(body)
    save_transaction(data, 'q.audit')
    logger.info(f"[AUDITORIA] {data['transaction_id']} | ${data['amount']} | {data.get('from_account')} → {data.get('to_account')}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_payment(ch, method, properties, body):
    data = json.loads(body)
    save_transaction(data, 'q.payments')
    logger.info(f"[PAGOS] {data['transaction_id']} | ${data['amount']} → {data.get('merchant')}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ── API Flask ────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'transaction-consumer', 'time': datetime.now().isoformat()})


@app.route('/transactions')
def list_transactions():
    limit  = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    tipo   = request.args.get('type')

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = "SELECT * FROM transactions"
            params = []
            if tipo:
                query += " WHERE type = %s"
                params.append(tipo.upper())
            query += " ORDER BY received_at DESC LIMIT %s OFFSET %s"
            params += [limit, offset]
            cur.execute(query, params)
            rows = cur.fetchall()
            return jsonify({'total': len(rows), 'offset': offset, 'transactions': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/transactions/stats')
def stats():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                             AS total,
                    COALESCE(SUM(amount), 0)             AS total_amount,
                    COALESCE(AVG(amount), 0)             AS avg_amount,
                    COUNT(*) FILTER (WHERE type='TRANSFER') AS transfers,
                    COUNT(*) FILTER (WHERE type='PAYMENT')  AS payments
                FROM transactions
            """)
            return jsonify(dict(cur.fetchone()))
    finally:
        conn.close()


@app.route('/transactions/<txn_id>')
def get_transaction(txn_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM transactions WHERE transaction_id = %s", (txn_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'error': 'Not found'}), 404
            return jsonify(dict(row))
    finally:
        conn.close()


# ── Consumer thread ──────────────────────────────────────────────────────────

def run_consumer():
    while True:
        try:
            logger.info("Consumer conectando a RabbitMQ...")
            conn = get_connection()
            ch   = conn.channel()
            setup_topology(ch)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_AUDIT,    on_message_callback=on_transfer)
            ch.basic_consume(queue=QUEUE_PAYMENTS, on_message_callback=on_payment)
            logger.info("Consumer escuchando: q.audit | q.payments")
            ch.start_consuming()
        except Exception as e:
            logger.error(f"Consumer error: {e}. Reintentando en 5s...")
            time.sleep(5)


# ── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_db()
    t = threading.Thread(target=run_consumer, daemon=True)
    t.start()
    logger.info("API Flask escuchando en :5001")
    app.run(host='0.0.0.0', port=5001)
