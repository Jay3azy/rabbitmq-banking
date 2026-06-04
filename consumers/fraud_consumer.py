"""
fraud_consumer.py  —  Microservicio Consumidor 3 (Direct)
Consume eventos de fraude alta/baja prioridad, los guarda en PostgreSQL
y expone una API Flask para consultarlos.

Rutas:
  GET /health          → estado del servicio
  GET /fraud           → lista todos los eventos (?priority=high|low)
  GET /fraud/<id>      → detalle de un evento
  GET /fraud/stats     → conteos y score promedio por prioridad
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
from config.rabbitmq_config import get_connection, setup_topology, QUEUE_FRAUD_HI, QUEUE_FRAUD_LO

logger = logging.getLogger('FraudConsumer')

DB_URL = os.getenv('DATABASE_URL', 'postgresql://admin:admin123@localhost:5432/fraud_db')


def get_db():
    return psycopg2.connect(DB_URL)


def init_db():
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fraud_events (
                    id           SERIAL PRIMARY KEY,
                    event_id     VARCHAR(50) UNIQUE NOT NULL,
                    account      VARCHAR(20),
                    score        NUMERIC(5,3),
                    high_priority BOOLEAN,
                    action_taken VARCHAR(50),
                    received_at  TIMESTAMP DEFAULT NOW(),
                    raw_message  JSONB
                )
            """)
    conn.close()
    logger.info("Base de datos inicializada ✓")


def save_fraud(data, high_priority):
    action = 'CUENTA_BLOQUEADA' if high_priority else 'MARCADO_REVISION'
    conn = get_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fraud_events (event_id, account, score, high_priority, action_taken, raw_message)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    data.get('event_id'),
                    data.get('account'),
                    data.get('score'),
                    high_priority,
                    action,
                    json.dumps(data)
                ))
    finally:
        conn.close()


# ── Callbacks ────────────────────────────────────────────────────────────────

def on_high_fraud(ch, method, properties, body):
    data = json.loads(body)
    save_fraud(data, True)
    logger.info(f"[FRAUDE ALTA] 🚨 BLOQUEADA: {data['account']} | score={data['score']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_low_fraud(ch, method, properties, body):
    data = json.loads(body)
    save_fraud(data, False)
    logger.info(f"[FRAUDE BAJA] 🔍 Revisión: {data['account']} | score={data['score']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ── API Flask ────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'fraud-consumer', 'time': datetime.now().isoformat()})


@app.route('/fraud')
def list_fraud():
    limit    = int(request.args.get('limit', 50))
    priority = request.args.get('priority')

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where, params = '', []
            if priority == 'high':
                where = 'WHERE high_priority = TRUE'
            elif priority == 'low':
                where = 'WHERE high_priority = FALSE'
            cur.execute(f"SELECT * FROM fraud_events {where} ORDER BY received_at DESC LIMIT %s", params + [limit])
            rows = cur.fetchall()
            return jsonify({'total': len(rows), 'events': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/fraud/stats')
def stats():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                         AS total,
                    COUNT(*) FILTER (WHERE high_priority = TRUE)     AS high_priority,
                    COUNT(*) FILTER (WHERE high_priority = FALSE)    AS low_priority,
                    ROUND(AVG(score)::numeric, 3)                    AS avg_score,
                    ROUND(AVG(score) FILTER (WHERE high_priority = TRUE)::numeric,  3) AS avg_score_high,
                    ROUND(AVG(score) FILTER (WHERE high_priority = FALSE)::numeric, 3) AS avg_score_low
                FROM fraud_events
            """)
            return jsonify(dict(cur.fetchone()))
    finally:
        conn.close()


@app.route('/fraud/<event_id>')
def get_fraud(event_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM fraud_events WHERE event_id = %s", (event_id,))
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
            conn = get_connection()
            ch   = conn.channel()
            setup_topology(ch)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_FRAUD_HI, on_message_callback=on_high_fraud)
            ch.basic_consume(queue=QUEUE_FRAUD_LO, on_message_callback=on_low_fraud)
            logger.info("Consumer escuchando: q.fraud.high | q.fraud.low")
            ch.start_consuming()
        except Exception as e:
            logger.error(f"Consumer error: {e}. Reintentando en 5s...")
            time.sleep(5)


if __name__ == '__main__':
    init_db()
    t = threading.Thread(target=run_consumer, daemon=True)
    t.start()
    logger.info("API Flask escuchando en :5003")
    app.run(host='0.0.0.0', port=5003)
