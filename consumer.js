import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'kafka-consumer-example',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'example-group' });

export async function consumeMessages(topic, onMessage) {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`📨 [${topic}:${partition}] Key: ${message.key}, Value: ${message.value}`);
      if (onMessage) {
        try {
          onMessage(JSON.parse(message.value.toString()));
        } catch (err) {
          console.error('Failed to parse message value as JSON:', err.message);
        }
      }
    },
  });
}

export async function consumeExample() {
  await consumeMessages('events', (msg) => {
    console.log('🔔 Received event:', msg);
  });
}
