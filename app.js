import express from 'express';
import { produceMessage } from './producer.js';
import { consumeMessages } from './consumer.js';

const app = express();
app.use(express.json());

const messageLog = [];

// Start consuming in background
consumeMessages('events', (msg) => {
  messageLog.push({ ...msg, receivedAt: new Date() });
  if (messageLog.length > 100) messageLog.shift();
}).catch(err => console.error('Consumer error:', err));

// REST endpoint to produce a message
app.post('/produce', async (req, res) => {
  try {
    const { topic = 'events', messages } = req.body;
    const payload = messages || [
      { key: 'user-1', value: { action: 'login', userId: 'user-1', timestamp: new Date() } },
    ];
    const result = await produceMessage(topic, payload);
    res.json({ success: true, result });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// REST endpoint to view recent messages
app.get('/messages', (req, res) => {
  res.json({ messages: messageLog.slice(-20) });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Kafka example app running on port ${PORT}`);
  console.log(`📊 Messages: http://localhost:${PORT}/messages`);
});
