// src/queue/Consumer.js - Nhận dữ liệu từ RabbitMQ và đưa vào Staging
const rabbitMQ = require('../../config/rabbitmq');
const logger = require('../utils/Logger');
const StagingService = require('../staging/StagingService');

class Consumer {
  constructor() {
    this.channel = null;
    this.stagingService = new StagingService();
    this.processedMessages = 0;
    this.totalRecords = 0;
  }

  async initialize() {
    this.channel = rabbitMQ.getChannel();
    await this.stagingService.initialize();
    await logger.info('Consumer initialized');
  }

  /**
   * Consume messages từ một queue
   */
  async consumeQueue(queueName) {
    try {
      await logger.startPhase(`CONSUMING QUEUE: ${queueName}`);

      return new Promise((resolve, reject) => {
        this.channel.consume(
          queueName,
          async (msg) => {
            if (msg) {
              try {
                const message = JSON.parse(msg.content.toString());
                await this.processMessage(message);
                
                // Acknowledge message
                this.channel.ack(msg);
                this.processedMessages++;
                this.totalRecords += message.recordCount || 0;

              } catch (error) {
                await logger.error('Error processing message', error, {
                  queue: queueName,
                  message: msg.content.toString().substring(0, 200)
                });
                
                // Reject và requeue nếu cần
                this.channel.nack(msg, false, false);
              }
            }
          },
          { noAck: false }
        );

        // Giả sử consume trong 5 giây (hoặc đến khi queue rỗng)
        setTimeout(async () => {
          await logger.endPhase(`CONSUMING QUEUE: ${queueName}`, {
            processedMessages: this.processedMessages,
            totalRecords: this.totalRecords
          });
          resolve();
        }, 5000);
      });

    } catch (error) {
      await logger.error(`Failed to consume queue: ${queueName}`, error);
      throw error;
    }
  }

  /**
   * Xử lý một message
   */
  async processMessage(message) {
    const { source, table, records, timestamp } = message;

    await logger.info(`Processing message from ${source}`, {
      table,
      recordCount: records.length,
      timestamp
    });

    // Lưu vào Staging database
    await this.stagingService.insertRecords(table, records, source);

    await logger.success(`✓ Processed ${records.length} records from ${table}`);
  }

  /**
   * Consume cả 2 queues (PostgreSQL và CSV)
   */
  async consumeAll() {
    await logger.startPhase('CONSUME - All Data Sources');

    try {
      // Consume PostgreSQL queue
      const ds1Queue = rabbitMQ.getQueueName('ds1');
      await this.consumeQueue(ds1Queue);

      // Consume CSV queue
      const ds2Queue = rabbitMQ.getQueueName('ds2');
      await this.consumeQueue(ds2Queue);

      await logger.endPhase('CONSUME - All Data Sources', {
        totalMessages: this.processedMessages,
        totalRecords: this.totalRecords
      });

      return {
        messagesProcessed: this.processedMessages,
        recordsProcessed: this.totalRecords
      };

    } catch (error) {
      await logger.error('Failed to consume all queues', error);
      throw error;
    }
  }

  /**
   * Consume với callback tùy chỉnh
   */
  async consumeWithCallback(queueName, callback) {
    await this.channel.consume(
      queueName,
      async (msg) => {
        if (msg) {
          try {
            const message = JSON.parse(msg.content.toString());
            await callback(message);
            this.channel.ack(msg);
          } catch (error) {
            await logger.error('Error in custom callback', error);
            this.channel.nack(msg, false, false);
          }
        }
      },
      { noAck: false }
    );
  }

  /**
   * Kiểm tra số message trong queue
   */
  async getQueueStats(queueName) {
    try {
      const info = await this.channel.checkQueue(queueName);
      await logger.info(`Queue stats for ${queueName}`, {
        messageCount: info.messageCount,
        consumerCount: info.consumerCount
      });
      return info;
    } catch (error) {
      await logger.error(`Failed to get queue stats: ${queueName}`, error);
      return null;
    }
  }

  /**
   * Purge queue (xóa tất cả messages)
   */
  async purgeQueue(queueName) {
    try {
      await this.channel.purgeQueue(queueName);
      await logger.warn(`Queue purged: ${queueName}`);
    } catch (error) {
      await logger.error(`Failed to purge queue: ${queueName}`, error);
    }
  }
}

module.exports = Consumer;