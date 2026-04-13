'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

// Lightweight unit tests that do not require a running Kafka broker.
// They verify module structure and the example-event generator.

describe('KafkaProducer', () => {
  let KafkaProducer;

  before(() => {
    // Stub kafkajs so we never touch a real broker
    require.cache[require.resolve('kafkajs')] = {
      id: require.resolve('kafkajs'),
      filename: require.resolve('kafkajs'),
      loaded: true,
      exports: {
        Kafka: class {
          producer() {
            return { connect: async () => {}, disconnect: async () => {}, send: async () => ({}) };
          }
          admin() {
            return {
              connect: async () => {},
              listTopics: async () => [],
              createTopics: async () => {},
              disconnect: async () => {},
            };
          }
        },
        Partitioners: { LegacyPartitioner: () => {} },
        CompressionTypes: { None: 0 },
      },
    };
    KafkaProducer = require('./producer');
  });

  after(() => {
    delete require.cache[require.resolve('kafkajs')];
    delete require.cache[require.resolve('./producer')];
  });

  it('generates 3 example events', () => {
    const p = new KafkaProducer();
    const events = p.generateExampleEvents();
    assert.equal(events.length, 3);
  });

  it('example events have login, purchase, logout types', () => {
    const p = new KafkaProducer();
    const events = p.generateExampleEvents();
    const types = events.map((e) => e.value.type);
    assert.deepEqual(types, ['login', 'purchase', 'logout']);
  });

  it('all example events share the same userId key', () => {
    const p = new KafkaProducer();
    const events = p.generateExampleEvents();
    const keys = events.map((e) => e.key);
    assert.equal(keys[0], keys[1]);
    assert.equal(keys[1], keys[2]);
  });
});

describe('KafkaConsumer', () => {
  let KafkaConsumer;

  before(() => {
    require.cache[require.resolve('kafkajs')] = {
      id: require.resolve('kafkajs'),
      filename: require.resolve('kafkajs'),
      loaded: true,
      exports: {
        Kafka: class {
          consumer() {
            return {
              connect: async () => {},
              subscribe: async () => {},
              run: async () => {},
              disconnect: async () => {},
            };
          }
        },
        Partitioners: { LegacyPartitioner: () => {} },
        CompressionTypes: { None: 0 },
      },
    };
    KafkaConsumer = require('./consumer');
  });

  after(() => {
    delete require.cache[require.resolve('kafkajs')];
    delete require.cache[require.resolve('./consumer')];
  });

  it('starts with empty message buffer', () => {
    const c = new KafkaConsumer();
    assert.equal(c.getMessages().length, 0);
  });

  it('getStats returns expected fields', () => {
    const c = new KafkaConsumer();
    const stats = c.getStats();
    assert.ok('totalReceived' in stats);
    assert.ok('buffered' in stats);
    assert.ok('maxBuffer' in stats);
    assert.ok('groupId' in stats);
    assert.ok('topic' in stats);
  });
});
