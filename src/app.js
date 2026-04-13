'use strict';

const express = require('express');
const KafkaProducer = require('./producer');
const KafkaConsumer = require('./consumer');

const app = express();
app.use(express.json());

const producer = new KafkaProducer();
const consumer = new KafkaConsumer();

const startTime = Date.now();
let ready = false;

async function init() {
  try {
    await producer.connect();
    await consumer.connect();
    ready = true;
    console.log('Kafka producer and consumer ready');
  } catch (err) {
    console.error('Kafka init error:', err.message);
    process.exit(1);
  }
}

// POST /produce - send a single message
app.post('/produce', async (req, res) => {
  if (!ready) {
    return res.status(503).json({ error: 'Service not ready' });
  }
  const { key, value } = req.body;
  if (value === undefined) {
    return res.status(400).json({ error: 'Missing required field: value' });
  }
  try {
    const result = await producer.send(key, value);
    res.json({ success: true, result });
  } catch (err) {
    console.error('Produce error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /produce-examples - generate and send sample events
app.post('/produce-examples', async (req, res) => {
  if (!ready) {
    return res.status(503).json({ error: 'Service not ready' });
  }
  try {
    const events = producer.generateExampleEvents();
    const result = await producer.sendBatch(events);
    res.json({ success: true, sent: events.length, result });
  } catch (err) {
    console.error('Produce examples error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /messages - view recent messages
app.get('/messages', (req, res) => {
  const messages = consumer.getMessages();
  res.json({ count: messages.length, messages });
});

// GET /stats - service statistics
app.get('/stats', (req, res) => {
  const consumerStats = consumer.getStats();
  res.json({
    uptime: Math.floor((Date.now() - startTime) / 1000),
    ready,
    kafka: consumerStats,
  });
});

// GET /health - health check
app.get('/health', (req, res) => {
  if (!ready) {
    return res.status(503).json({ status: 'unavailable' });
  }
  res.json({ status: 'ok', uptime: Math.floor((Date.now() - startTime) / 1000) });
});

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  init();
});

async function shutdown(signal) {
  console.log(`Received ${signal}, shutting down…`);
  server.close(async () => {
    try {
      await producer.disconnect();
      await consumer.disconnect();
    } catch (err) {
      console.error('Shutdown error:', err.message);
    }
    process.exit(0);
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

module.exports = { app, server };
