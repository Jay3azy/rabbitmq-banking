import os
import pika
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)

RABBITMQ_HOST  = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT  = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', 'banking')
RABBITMQ_USER  = os.getenv('RABBITMQ_USER', 'admin')
RABBITMQ_PASS  = os.getenv('RABBITMQ_PASS', 'admin123')

EXCHANGE_TOPIC  = 'bank.transactions'
EXCHANGE_FANOUT = 'bank.alerts'
EXCHANGE_DIRECT = 'bank.priority'

QUEUE_AUDIT    = 'q.audit'
QUEUE_PAYMENTS = 'q.payments'
QUEUE_EMAIL    = 'q.alert.email'
QUEUE_SMS      = 'q.alert.sms'
QUEUE_FRAUD_HI = 'q.fraud.high'
QUEUE_FRAUD_LO = 'q.fraud.low'

RK_FRAUD_HIGH = 'fraud.high'
RK_FRAUD_LOW  = 'fraud.low'


def get_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)


def setup_topology(channel):
    logger = logging.getLogger('setup')
    channel.exchange_declare(exchange=EXCHANGE_TOPIC,  exchange_type='topic',  durable=True)
    channel.exchange_declare(exchange=EXCHANGE_FANOUT, exchange_type='fanout', durable=True)
    channel.exchange_declare(exchange=EXCHANGE_DIRECT, exchange_type='direct', durable=True)

    for q in [QUEUE_AUDIT, QUEUE_PAYMENTS, QUEUE_EMAIL, QUEUE_SMS, QUEUE_FRAUD_HI, QUEUE_FRAUD_LO]:
        channel.queue_declare(queue=q, durable=True)

    channel.queue_bind(queue=QUEUE_AUDIT,    exchange=EXCHANGE_TOPIC,  routing_key='txn.transfer.#')
    channel.queue_bind(queue=QUEUE_PAYMENTS, exchange=EXCHANGE_TOPIC,  routing_key='txn.payment.#')
    channel.queue_bind(queue=QUEUE_EMAIL,    exchange=EXCHANGE_FANOUT, routing_key='')
    channel.queue_bind(queue=QUEUE_SMS,      exchange=EXCHANGE_FANOUT, routing_key='')
    channel.queue_bind(queue=QUEUE_FRAUD_HI, exchange=EXCHANGE_DIRECT, routing_key=RK_FRAUD_HIGH)
    channel.queue_bind(queue=QUEUE_FRAUD_LO, exchange=EXCHANGE_DIRECT, routing_key=RK_FRAUD_LOW)

    logger.info("Topología configurada correctamente ✓")
