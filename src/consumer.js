'use strict';

const { Kafka } = require('kafkajs');

const TOPIC = 'events';
const GROUP_ID = 'railway-consumer-group';
const MAX_BUFFER = 100;

class KafkaConsumer {
  constructor() {
    const broker = process.env.KAFKA_BROKER || 'localhost:9092';
    this.kafka = new Kafka({
      clientId: 'railway-kafka-consumer',
      brokers: [broker],
      retry: {
        initialRetryTime: 300,
        retries: 10,
      },
    });
    this.consumer = this.kafka.consumer({
      groupId: GROUP_ID,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });
    this.messages = [];
    this.totalReceived = 0;
    this.connected = false;
  }

  async connect() {
    await this.consumer.connect();
    this.connected = true;
    console.log('Consumer connected');

    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await this.consumer.run({
      autoCommit: true,
      autoCommitInterval: 5000,
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const raw = message.value ? message.value.toString() : null;
          const parsed = raw ? JSON.parse(raw) : null;
          const entry = {
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString() : null,
            value: parsed,
            receivedAt: new Date().toISOString(),
          };
          this.messages.push(entry);
          if (this.messages.length > MAX_BUFFER) {
            this.messages.shift();
          }
          this.totalReceived += 1;
        } catch (err) {
          console.error('Error processing message:', err.message);
        }
      },
    });
  }

  getMessages() {
    return [...this.messages];
  }

  getStats() {
    return {
      totalReceived: this.totalReceived,
      buffered: this.messages.length,
      maxBuffer: MAX_BUFFER,
      groupId: GROUP_ID,
      topic: TOPIC,
    };
  }

  async disconnect() {
    if (this.connected) {
      await this.consumer.disconnect();
      this.connected = false;
      console.log('Consumer disconnected');
    }
  }
}

module.exports = KafkaConsumer;
