# rabbitmq-banking 🏦

Sistema de Transacciones Bancarias implementado como **microservicios** con RabbitMQ, PostgreSQL y APIs REST.

**Materia:** ISWZ2202 - Diseño y Arquitectura de Software  
**Integrante:** José Ortiz

---

## Arquitectura

```
                        ┌─────────────────────────────────────────────────┐
                        │              RabbitMQ  (broker)                  │
                        │                                                   │
                        │  [bank.transactions] topic                        │
                        │    txn.transfer.# → q.audit                      │
                        │    txn.payment.#  → q.payments                   │
                        │                                                   │
                        │  [bank.alerts] fanout                             │
                        │    * → q.alert.email                              │
                        │    * → q.alert.sms                                │
                        │                                                   │
                        │  [bank.priority] direct                           │
                        │    fraud.high → q.fraud.high                      │
                        │    fraud.low  → q.fraud.low                       │
                        └─────────────────────────────────────────────────┘
                               ▲                    │
                               │ publica            │ consume
                        ┌──────┴──────┐             │
                        │  Producer   │      ┌──────┴────────────────────────┐
                        │ (loop 10s)  │      │                               │
                        └─────────────┘      ▼                               ▼
                                   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                                   │  Consumer 1     │    │  Consumer 2     │    │  Consumer 3     │
                                   │  Transacciones  │    │  Alertas        │    │  Fraude         │
                                   │  (Topic)        │    │  (Fanout)       │    │  (Direct)       │
                                   │  API :5001      │    │  API :5002      │    │  API :5003      │
                                   └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
                                            │                      │                      │
                                            ▼                      ▼                      ▼
                                   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
                                   │  PostgreSQL    │   │  PostgreSQL    │   │  PostgreSQL    │
                                   │  transactions  │   │  alerts_db     │   │  fraud_db      │
                                   └────────────────┘   └────────────────┘   └────────────────┘
```

### Microservicios

| Servicio | Descripción | Puerto API |
|---|---|---|
| `producer` | Publica 5 mensajes cada 10 segundos en bucle | — |
| `consumer_transactions` | Consume transferencias y pagos (Topic) | `5001` |
| `consumer_alerts` | Consume alertas email + SMS (Fanout) | `5002` |
| `consumer_fraud` | Consume fraude alta/baja prioridad (Direct) | `5003` |

### Patrones de mensajería

| Exchange | Tipo | Routing |
|---|---|---|
| `bank.transactions` | **Topic** | `txn.transfer.#` → auditoría / `txn.payment.#` → pagos |
| `bank.alerts` | **Fanout** | Broadcast simultáneo a email Y sms |
| `bank.priority` | **Direct** | `fraud.high` / `fraud.low` → cola exacta |

---

## Requisitos

- [Docker](https://www.docker.com/get-started) + [Docker Compose](https://docs.docker.com/compose/)

---

## Ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/Jay3azy/rabbitmq-banking.git
cd rabbitmq-banking
```

### 2. Levantar todos los servicios

```bash
docker compose up --build
```

Esto levanta automáticamente:
- RabbitMQ (con reintentos de salud)
- 3 bases de datos PostgreSQL independientes
- El productor (publica cada 10 segundos)
- Los 3 consumidores (cada uno con su API)

> Primera vez tarda ~2 minutos mientras se construyen las imágenes.

### 3. Verificar que todo corre

```bash
docker compose ps
```

Todos los servicios deben aparecer como `running`.

### 4. Detener

```bash
docker compose down          # detiene y elimina contenedores
docker compose down -v       # también elimina los volúmenes (borra datos)
```

---

## APIs REST

### Consumer 1 — Transacciones (`http://localhost:5001`)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado del servicio |
| GET | `/transactions` | Lista transacciones (acepta `?type=TRANSFER\|PAYMENT&limit=50`) |
| GET | `/transactions/stats` | Total, suma y promedio de transacciones |
| GET | `/transactions/<id>` | Detalle de una transacción por ID |

**Ejemplos:**
```bash
curl http://localhost:5001/transactions
curl http://localhost:5001/transactions?type=TRANSFER
curl http://localhost:5001/transactions/stats
```

---

### Consumer 2 — Alertas (`http://localhost:5002`)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado del servicio |
| GET | `/alerts` | Lista alertas (acepta `?channel=email\|sms&severity=HIGH`) |
| GET | `/alerts/stats` | Conteo por severidad y canal |
| GET | `/alerts/<id>` | Detalle de una alerta por ID |

**Ejemplos:**
```bash
curl http://localhost:5002/alerts
curl http://localhost:5002/alerts?severity=HIGH
curl http://localhost:5002/alerts/stats
```

---

### Consumer 3 — Fraude (`http://localhost:5003`)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado del servicio |
| GET | `/fraud` | Lista eventos de fraude (acepta `?priority=high\|low`) |
| GET | `/fraud/stats` | Conteo y score promedio por prioridad |
| GET | `/fraud/<id>` | Detalle de un evento por ID |

**Ejemplos:**
```bash
curl http://localhost:5003/fraud
curl http://localhost:5003/fraud?priority=high
curl http://localhost:5003/fraud/stats
```

---

## Panel de administración RabbitMQ

Accede a `http://localhost:15672`  
**Usuario:** `admin` | **Contraseña:** `admin123`

Desde ahí puedes ver los exchanges, colas y el flujo de mensajes en tiempo real.

---

## Estructura del proyecto

```
rabbitmq-banking/
├── docker-compose.yml           # Orquesta todos los microservicios
├── Dockerfile.producer          # Imagen del productor
├── Dockerfile.consumer          # Imagen base de los consumidores
├── requirements.txt             # Dependencias Python
├── wait_for_services.py         # Espera a que RabbitMQ/PG estén listos
├── config/
│   └── rabbitmq_config.py       # Configuración central (env vars)
├── producer/
│   └── bank_producer.py         # Loop de publicación de mensajes
└── consumers/
    ├── transaction_consumer.py  # Topic + API Flask :5001
    ├── alert_consumer.py        # Fanout + API Flask :5002
    └── fraud_consumer.py        # Direct + API Flask :5003
```

---

## Cómo cumple los requisitos

| Requisito | Implementación |
|---|---|
| **Desplegarse solo** | `docker compose up --build` levanta todo automáticamente |
| **Comunicación asíncrona** | RabbitMQ como message broker entre producer y consumers |
| **Almacenamiento propio** | Cada consumer tiene su propia base PostgreSQL independiente |
| **Exponer APIs** | Los 3 consumers exponen APIs REST Flask con endpoints de consulta |
