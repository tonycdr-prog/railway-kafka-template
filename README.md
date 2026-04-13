# Apache Kafka Template for Railway

Deploy a production-ready Apache Kafka cluster on Railway with a Node.js producer/consumer example.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new?template=https://github.com/tonycdr-prog/railway-kafka-template)

## Features

- **Apache Kafka** — Distributed event streaming platform
- **Node.js Example App** — Producer & consumer with REST API
- **KafkaJS** — Modern Kafka client for Node.js
- **One-click deployment** via Railway button

## What It Does

- Produces sample user events (login, purchase, logout)
- Consumes events in real-time and keeps an in-memory log
- Exposes REST endpoints for production integration

## Quick Start

### Local Development

```bash
npm install
npm run dev
```

### Produce a message

```bash
curl -X POST http://localhost:3000/produce \
  -H "Content-Type: application/json" \
  -d '{"topic":"events","messages":[{"key":"user-1","value":{"action":"login","userId":"user-1"}}]}'
```

### View recent messages

```bash
curl http://localhost:3000/messages
```

### Health check

```bash
curl http://localhost:3000/health
```

## Topics

Default topic: `events`

### Message Schema

```json
{
  "key": "user-123",
  "value": {
    "action": "login | purchase | logout",
    "userId": "user-123",
    "amount": 99.99,
    "timestamp": "2026-04-13T10:30:00Z"
  }
}
```

## Project Structure

```
railway-kafka-template/
├── app.js          # Express server with produce/consume endpoints
├── producer.js     # KafkaJS producer helper
├── consumer.js     # KafkaJS consumer helper
├── package.json    # Node.js dependencies
└── railway.json    # Railway deployment config
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `NODE_ENV` | `development` | Environment |
| `PORT` | `3000` | Express app port |

## Deploy to Railway

1. Click the **Deploy on Railway** button above, or
2. Fork this repo, create a Railway project, and connect your fork
3. Add a **Kafka** plugin from the Railway marketplace
4. Set `KAFKA_BROKER` to the Railway-provided Kafka URL

## Use Cases

- **Event Streaming** — Real-time data pipelines
- **Log Aggregation** — Centralized logging
- **Pub/Sub Messaging** — Decoupled microservices
- **Stream Processing** — Data transformation at scale

## Next Steps

- Customize topic names for your domain
- Add message validation & schema registry
- Scale consumer groups for parallel processing
- Persist events to a database (PostgreSQL, Redis)
