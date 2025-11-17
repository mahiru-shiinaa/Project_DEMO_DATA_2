// config/rabbitmq.js - RabbitMQ Connection Manager
const amqp = require('amqplib');
require('dotenv').config();

class RabbitMQManager {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.queues = {
      ds1: process.env.QUEUE_NAME_DS1 || 'datasource1_queue',
      ds2: process.env.QUEUE_NAME_DS2 || 'datasource2_queue',
      staging: process.env.QUEUE_NAME_STAGING || 'staging_queue'
    };
  }

  async connect() {
    try {
      this.connection = await amqp.connect(process.env.RABBITMQ_URL);
      this.channel = await this.connection.createChannel();
      
      // Khai báo các queues
      await this.channel.assertQueue(this.queues.ds1, { durable: true });
      await this.channel.assertQueue(this.queues.ds2, { durable: true });
      await this.channel.assertQueue(this.queues.staging, { durable: true });

      console.log('✅ Connected to RabbitMQ');
      console.log(`📫 Queues initialized: ${Object.values(this.queues).join(', ')}`);

      // Handle connection errors
      this.connection.on('error', (err) => {
        console.error('❌ RabbitMQ connection error:', err.message);
      });

      this.connection.on('close', () => {
        console.log('🔌 RabbitMQ connection closed');
      });

      return this.channel;
    } catch (error) {
      console.error('❌ Failed to connect to RabbitMQ:', error.message);
      throw error;
    }
  }

  getChannel() {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized. Call connect() first.');
    }
    return this.channel;
  }

  getQueueName(source) {
    return this.queues[source];
  }

  async close() {
    if (this.channel) {
      await this.channel.close();
    }
    if (this.connection) {
      await this.connection.close();
    }
    console.log('🔌 RabbitMQ connections closed');
  }
}

module.exports = new RabbitMQManager();