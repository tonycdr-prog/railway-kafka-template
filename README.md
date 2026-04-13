# railway-kafka-template

A production-ready Apache Kafka distributed event streaming template for [Railway](https://railway.app), with a Node.js REST API for producing and consuming messages.

## Quick Start

### 1 – Deploy to Railway

Click **Deploy on Railway** and the `railroad.json` manifest will spin up:

- **Zookeeper** (port 2181)
- **Kafka broker** (port 9092)
- **Node.js app** (port 3000)

Environment variables are wired automatically between services.

### 2 – Local Development

```bash
# Clone and install
git clone https://github.com/tonycdr-prog/railway-kafka-template.git
cd railway-kafka-template
npm install

# Copy and edit environment config
cp .env.example .env

# Start a local Kafka (requires Docker)
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:7.5.0 \
  -e ZOOKEEPER_CLIENT_PORT=2181

docker run -d --name kafka -p 9092:9092 --link zookeeper confluentinc/cp-kafka:7.5.0 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1

# Run the app
npm run dev
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/produce` | Send a single message to Kafka |
| `POST` | `/produce-examples` | Generate and send sample login/purchase/logout events |
| `GET` | `/messages` | View the last 100 consumed messages |
| `GET` | `/stats` | Consumer group and uptime statistics |
| `GET` | `/health` | Health check (returns `200 ok` when ready) |

### POST /produce

```json
{
  "key": "user_123",
  "value": {
    "type": "custom_event",
    "data": "anything"
  }
}
```

### POST /produce-examples

No body required. Generates a `login → purchase → logout` sequence for a random user.

### GET /messages

```json
{
  "count": 3,
  "messages": [
    {
      "topic": "events",
      "partition": 0,
      "offset": "0",
      "key": "user_4521",
      "value": { "type": "login", "userId": "user_4521", "timestamp": "2024-01-01T00:00:00.000Z" },
      "receivedAt": "2024-01-01T00:00:00.100Z"
    }
  ]
}
```

### GET /stats

```json
{
  "uptime": 120,
  "ready": true,
  "kafka": {
    "totalReceived": 42,
    "buffered": 42,
    "maxBuffer": 100,
    "groupId": "railway-consumer-group",
    "topic": "events"
  }
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Railway                     │
│                                              │
│  ┌──────────┐   ┌──────────┐   ┌─────────┐  │
│  │Zookeeper │──▶│  Kafka   │◀──│ Node.js │  │
│  │:2181     │   │  :9092   │──▶│  :3000  │  │
│  └──────────┘   └──────────┘   └─────────┘  │
│                                   REST API   │
└─────────────────────────────────────────────┘
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `NODE_ENV` | `development` | Runtime environment |
| `PORT` | `3000` | HTTP server port |

## Key Features

- **One-click Railway deployment** via `railroad.json`
- **Auto-creating Kafka topics** with 24-hour retention
- **REST API** for producing and consuming messages
- **Consumer group management** with auto-commit offsets
- **Idempotent producer** with retry logic
- **Message buffering** – last 100 messages kept in memory
- **Health checks** for Railway and Docker
- **Graceful shutdown** (SIGTERM / SIGINT)
- **Non-root Docker container** for security

## Use Cases

- Real-time user event tracking (login, purchase, logout)
- Microservice event bus
- Log aggregation pipeline
- Notification queues
- IoT data ingestion

## Running Tests

```bash
npm test
```

## License

MIT