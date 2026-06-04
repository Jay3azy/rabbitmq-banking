"""
alert_consumer.py  —  Microservicio Consumidor 2 (Fanout)
Consume alertas de seguridad (email + SMS), las guarda en PostgreSQL
y expone una API Flask para consultarlas.

Rutas:
  GET /health          → estado del servicio
  GET /alerts          → lista todas las alertas (?channel=email|sms&severity=HIGH)
  GET /alerts/<id>     → detalle de una alerta
  GET /alerts/stats    → conteos por severidad y canal
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
from config.rabbitmq_config import get_connection, setup_topology, QUEUE_EMAIL, QUEUE_SMS

logger = logging.getLogger('AlertConsumer')

DB_URL = os.getenv('DATABASE_URL', 'postgresql://admin:admin123@localhost:5432/alerts_db')


def get_db():
    return psycopg2.connect(DB_URL)


def init_db():
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id          SERIAL PRIMARY KEY,
                    alert_id    VARCHAR(50) NOT NULL,
                    alert_type  VARCHAR(60) NOT NULL,
                    account     VARCHAR(20),
                    severity    VARCHAR(10),
                    channel     VARCHAR(10) NOT NULL,
                    received_at TIMESTAMP DEFAULT NOW(),
                    raw_message JSONB
                )
            """)
    conn.close()
    logger.info("Base de datos inicializada ✓")


def save_alert(data, channel):
    conn = get_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alerts (alert_id, alert_type, account, severity, channel, raw_message)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (
                    data.get('alert_id'),
                    data.get('alert_type'),
                    data.get('account'),
                    data.get('severity'),
                    channel,
                    json.dumps(data)
                ))
    finally:
        conn.close()


# ── Callbacks ────────────────────────────────────────────────────────────────

def on_email(ch, method, properties, body):
    data = json.loads(body)
    save_alert(data, 'email')
    logger.info(f"[EMAIL] {data['alert_type']} | {data['severity']} | {data['account']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_sms(ch, method, properties, body):
    data = json.loads(body)
    save_alert(data, 'sms')
    logger.info(f"[SMS]   {data['alert_type']} | {data['severity']} | {data['account']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# ── API Flask ────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'alert-consumer', 'time': datetime.now().isoformat()})


@app.route('/alerts')
def list_alerts():
    limit    = int(request.args.get('limit', 50))
    channel  = request.args.get('channel')
    severity = request.args.get('severity')

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            filters, params = [], []
            if channel:
                filters.append("channel = %s"); params.append(channel.lower())
            if severity:
                filters.append("severity = %s"); params.append(severity.upper())
            where = ("WHERE " + " AND ".join(filters)) if filters else ""
            cur.execute(f"SELECT * FROM alerts {where} ORDER BY received_at DESC LIMIT %s", params + [limit])
            rows = cur.fetchall()
            return jsonify({'total': len(rows), 'alerts': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/alerts/stats')
def stats():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE severity='HIGH')   AS high,
                    COUNT(*) FILTER (WHERE severity='MEDIUM') AS medium,
                    COUNT(*) FILTER (WHERE severity='LOW')    AS low,
                    COUNT(*) FILTER (WHERE channel='email')   AS via_email,
                    COUNT(*) FILTER (WHERE channel='sms')     AS via_sms
                FROM alerts
            """)
            return jsonify(dict(cur.fetchone()))
    finally:
        conn.close()


@app.route('/alerts/<alert_id>')
def get_alert(alert_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM alerts WHERE alert_id = %s ORDER BY received_at DESC LIMIT 1", (alert_id,))
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
            ch.basic_consume(queue=QUEUE_EMAIL, on_message_callback=on_email)
            ch.basic_consume(queue=QUEUE_SMS,   on_message_callback=on_sms)
            logger.info("Consumer escuchando: q.alert.email | q.alert.sms")
            ch.start_consuming()
        except Exception as e:
            logger.error(f"Consumer error: {e}. Reintentando en 5s...")
            time.sleep(5)


if __name__ == '__main__':
    init_db()
    t = threading.Thread(target=run_consumer, daemon=True)
    t.start()
    logger.info("API Flask escuchando en :5002")
    app.run(host='0.0.0.0', port=5002)
