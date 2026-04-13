'use strict';

const { Kafka, Partitioners, CompressionTypes } = require('kafkajs');
const crypto = require('crypto');

const TOPIC = 'events';

class KafkaProducer {
  constructor() {
    const broker = process.env.KAFKA_BROKER || 'localhost:9092';
    this.kafka = new Kafka({
      clientId: 'railway-kafka-producer',
      brokers: [broker],
      retry: {
        initialRetryTime: 300,
        retries: 10,
      },
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
      idempotent: true,
      maxInFlightRequests: 1,
      transactionTimeout: 30000,
    });
    this.admin = this.kafka.admin();
    this.connected = false;
  }

  async connect() {
    await this.admin.connect();
    const topics = await this.admin.listTopics();
    if (!topics.includes(TOPIC)) {
      await this.admin.createTopics({
        topics: [
          {
            topic: TOPIC,
            numPartitions: 1,
            replicationFactor: 1,
            configEntries: [
              { name: 'retention.ms', value: String(24 * 60 * 60 * 1000) },
            ],
          },
        ],
      });
      console.log(`Topic "${TOPIC}" created`);
    }
    await this.admin.disconnect();

    await this.producer.connect();
    this.connected = true;
    console.log('Producer connected');
  }

  async send(key, value) {
    const message = {
      key: key != null ? String(key) : null,
      value: JSON.stringify({ ...value, timestamp: new Date().toISOString() }),
    };
    const result = await this.producer.send({
      topic: TOPIC,
      compression: CompressionTypes.None,
      messages: [message],
    });
    return result;
  }

  async sendBatch(messages) {
    const topicMessages = messages.map((msg) => ({
      key: msg.key != null ? String(msg.key) : null,
      value: JSON.stringify({ ...msg.value, timestamp: new Date().toISOString() }),
    }));
    const result = await this.producer.send({
      topic: TOPIC,
      compression: CompressionTypes.None,
      messages: topicMessages,
    });
    return result;
  }

  generateExampleEvents() {
    const rand = (max) => Math.floor(crypto.getRandomValues(new Uint32Array(1))[0] / 0x100000000 * max);
    const userId = `user_${rand(10000)}`;
    return [
      {
        key: userId,
        value: {
          type: 'login',
          userId,
          ip: `192.168.${rand(255)}.${rand(255)}`,
          userAgent: 'Mozilla/5.0',
        },
      },
      {
        key: userId,
        value: {
          type: 'purchase',
          userId,
          productId: `prod_${rand(1000)}`,
          amount: parseFloat((rand(200) + 5 + rand(100) / 100).toFixed(2)),
          currency: 'USD',
        },
      },
      {
        key: userId,
        value: {
          type: 'logout',
          userId,
          sessionDuration: rand(3600),
        },
      },
    ];
  }

  async disconnect() {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
      console.log('Producer disconnected');
    }
  }
}

module.exports = KafkaProducer;
