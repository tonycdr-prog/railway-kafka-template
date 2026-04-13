import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'kafka-producer-example',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer();

export async function produceMessage(topic, messages) {
  await producer.connect();
  try {
    const result = await producer.send({
      topic,
      messages: messages.map(msg => ({
        key: msg.key || null,
        value: JSON.stringify(msg.value),
        timestamp: Date.now().toString(),
      })),
    });
    console.log('✅ Produced:', result);
    return result;
  } finally {
    await producer.disconnect();
  }
}

export async function produceExample() {
  await produceMessage('events', [
    { key: 'user-1', value: { action: 'login', userId: 'user-1', timestamp: new Date() } },
    { key: 'user-2', value: { action: 'purchase', userId: 'user-2', amount: 99.99, timestamp: new Date() } },
    { key: 'user-1', value: { action: 'logout', userId: 'user-1', timestamp: new Date() } },
  ]);
}
