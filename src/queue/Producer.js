// src/queue/Producer.js - Đẩy dữ liệu vào RabbitMQ queues
const rabbitMQ = require('../../config/rabbitmq');
const logger = require('../utils/Logger');

class Producer {
  constructor() {
    this.channel = null;
  }

  async initialize() {
    this.channel = rabbitMQ.getChannel();
    await logger.info('Producer initialized');
  }

  /**
   * Gửi dữ liệu từ một data source vào queue
   * @param {string} queueName - Tên queue
   * @param {object} data - Dữ liệu cần gửi (object chứa các table)
   * @param {string} sourceType - 'postgresql' hoặc 'csv'
   */
  async sendToQueue(queueName, data, sourceType) {
    try {
      await logger.info(`Sending data to queue: ${queueName}`, {
        sourceType,
        tables: Object.keys(data),
        totalRecords: this.countTotalRecords(data)
      });

      // Gửi từng table như một message riêng
      for (const [tableName, records] of Object.entries(data)) {
        const message = {
          source: sourceType,
          table: tableName,
          records: records,
          timestamp: new Date().toISOString(),
          recordCount: records.length
        };

        const sent = this.channel.sendToQueue(
          queueName,
          Buffer.from(JSON.stringify(message)),
          { persistent: true }
        );

        if (sent) {
          await logger.success(`Sent ${records.length} records from ${tableName} to ${queueName}`);
        } else {
          await logger.warn(`Queue ${queueName} is full, waiting...`);
          // Đợi queue available
          await this.waitForDrain();
          // Retry
          this.channel.sendToQueue(
            queueName,
            Buffer.from(JSON.stringify(message)),
            { persistent: true }
          );
        }
      }

      await logger.success(`All data sent to queue: ${queueName}`);
      return true;
    } catch (error) {
      await logger.error(`Failed to send data to queue: ${queueName}`, error);
      throw error;
    }
  }

  /**
   * Gửi dữ liệu từ PostgreSQL data source
   */
  async sendPostgresData(data) {
    const queueName = rabbitMQ.getQueueName('ds1');
    return await this.sendToQueue(queueName, data, 'postgresql');
  }

  /**
   * Gửi dữ liệu từ CSV data source
   */
  async sendCsvData(data) {
    const queueName = rabbitMQ.getQueueName('ds2');
    return await this.sendToQueue(queueName, data, 'csv');
  }

  /**
   * Đếm tổng số records
   */
  countTotalRecords(data) {
    return Object.values(data).reduce((sum, records) => sum + records.length, 0);
  }

  /**
   * Đợi queue available
   */
  waitForDrain() {
    return new Promise((resolve) => {
      this.channel.once('drain', resolve);
    });
  }

  /**
   * Gửi batch data (cho big data)
   */
  async sendBatch(queueName, tableName, records, batchSize = 1000) {
    const batches = [];
    for (let i = 0; i < records.length; i += batchSize) {
      batches.push(records.slice(i, i + batchSize));
    }

    await logger.info(`Sending ${batches.length} batches to queue`, {
      totalRecords: records.length,
      batchSize
    });

    for (let i = 0; i < batches.length; i++) {
      const message = {
        table: tableName,
        records: batches[i],
        batch: i + 1,
        totalBatches: batches.length,
        timestamp: new Date().toISOString()
      };

      this.channel.sendToQueue(
        queueName,
        Buffer.from(JSON.stringify(message)),
        { persistent: true }
      );

      await logger.debug(`Sent batch ${i + 1}/${batches.length} for ${tableName}`);
    }

    await logger.success(`All batches sent for ${tableName}`);
  }
}

module.exports = Producer;