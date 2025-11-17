// config/database.js - Quản lý kết nối PostgreSQL
const { Pool } = require('pg');
require('dotenv').config();

class DatabaseManager {
  constructor() {
    // Data Source 1 - E-commerce Database
    this.ds1Pool = new Pool({
      host: process.env.DS1_HOST,
      database: process.env.DS1_DATABASE,
      user: process.env.DS1_USER,
      password: process.env.DS1_PASSWORD,
      port: process.env.DS1_PORT,
      ssl: process.env.DS1_SSL === 'true' ? { rejectUnauthorized: false } : false,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    // Staging Database
    this.stagingPool = new Pool({
      host: process.env.DS2_HOST,
      database: process.env.DS2_DATABASE,
      user: process.env.DS2_USER,
      password: process.env.DS2_PASSWORD,
      port: process.env.DS2_PORT,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    // Data Warehouse (Final Clean Data)
    this.dwPool = new Pool({
      host: process.env.DW_HOST,
      database: process.env.DW_DATABASE,
      user: process.env.DW_USER,
      password: process.env.DW_PASSWORD,
      port: process.env.DW_PORT,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    this.testConnections();
  }

  async testConnections() {
    try {
      // Test Data Source 1
      const ds1Client = await this.ds1Pool.connect();
      console.log('✅ Connected to Data Source 1 (E-commerce DB)');
      ds1Client.release();

      // Test Staging
      const stagingClient = await this.stagingPool.connect();
      console.log('✅ Connected to Staging Database');
      stagingClient.release();

      // Test Data Warehouse
      const dwClient = await this.dwPool.connect();
      console.log('✅ Connected to Data Warehouse');
      dwClient.release();
    } catch (error) {
      console.error('❌ Database connection error:', error.message);
      throw error;
    }
  }

  // Get Data Source 1 connection
  getDS1Pool() {
    return this.ds1Pool;
  }

  // Get Staging connection
  getStagingPool() {
    return this.stagingPool;
  }

  // Get Data Warehouse connection
  getDWPool() {
    return this.dwPool;
  }

  // Close all connections
  async closeAll() {
    await this.ds1Pool.end();
    await this.stagingPool.end();
    await this.dwPool.end();
    console.log('🔌 All database connections closed');
  }
}

module.exports = new DatabaseManager();