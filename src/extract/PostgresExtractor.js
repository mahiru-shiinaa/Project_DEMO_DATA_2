// src/extract/PostgresExtractor.js - Đọc dữ liệu từ PostgreSQL
const dbManager = require('../../config/database');
const logger = require('../utils/Logger');

class PostgresExtractor {
  constructor() {
    this.pool = dbManager.getDS1Pool();
    this.tables = [
      'khach_hang',
      'danh_muc',
      'san_pham',
      'don_hang',
      'chi_tiet_don_hang',
      'thanh_toan'
    ];
  }

  // Extract tất cả dữ liệu từ Data Source 1
  async extractAll() {
    await logger.startPhase('EXTRACT - PostgreSQL Data Source 1');
    
    const results = {};
    let totalRecords = 0;

    try {
      for (const tableName of this.tables) {
        await logger.info(`Extracting table: ${tableName}`);
        
        const data = await this.extractTable(tableName);
        results[tableName] = data;
        totalRecords += data.length;

        await logger.success(`✓ Extracted ${data.length} records from ${tableName}`);
      }

      await logger.endPhase('EXTRACT - PostgreSQL Data Source 1', {
        totalTables: this.tables.length,
        totalRecords: totalRecords
      });

      return results;
    } catch (error) {
      await logger.error('Failed to extract from PostgreSQL', error);
      throw error;
    }
  }

  // Extract một bảng cụ thể
  async extractTable(tableName) {
    try {
      const query = `SELECT * FROM ${tableName}`;
      const result = await this.pool.query(query);
      
      return result.rows;
    } catch (error) {
      await logger.error(`Error extracting table ${tableName}`, error);
      throw error;
    }
  }

  // Extract với điều kiện
  async extractWithCondition(tableName, condition, params = []) {
    try {
      const query = `SELECT * FROM ${tableName} WHERE ${condition}`;
      const result = await this.pool.query(query, params);
      
      await logger.info(`Extracted ${result.rows.length} records from ${tableName} with condition`);
      return result.rows;
    } catch (error) {
      await logger.error(`Error extracting ${tableName} with condition`, error);
      throw error;
    }
  }

  // Extract theo batch (phân trang)
  async extractBatch(tableName, batchSize = 1000, offset = 0) {
    try {
      const query = `SELECT * FROM ${tableName} LIMIT $1 OFFSET $2`;
      const result = await this.pool.query(query, [batchSize, offset]);
      
      return result.rows;
    } catch (error) {
      await logger.error(`Error extracting batch from ${tableName}`, error);
      throw error;
    }
  }

  // Đếm tổng số records
  async countRecords(tableName) {
    try {
      const query = `SELECT COUNT(*) as total FROM ${tableName}`;
      const result = await this.pool.query(query);
      return parseInt(result.rows[0].total);
    } catch (error) {
      await logger.error(`Error counting records in ${tableName}`, error);
      return 0;
    }
  }

  // Extract với JOIN (nếu cần)
  async extractWithJoin(query) {
    try {
      const result = await this.pool.query(query);
      return result.rows;
    } catch (error) {
      await logger.error('Error executing join query', error);
      throw error;
    }
  }
}

module.exports = PostgresExtractor;