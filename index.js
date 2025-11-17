// index.js - Main ETL Pipeline Entry Point
require('dotenv').config();
const logger = require('./src/utils/Logger');
const dbManager = require('./config/database');
const rabbitMQ = require('./config/rabbitmq');

// Extract
const PostgresExtractor = require('./src/extract/PostgresExtractor');
const CsvExtractor = require('./src/extract/CsvExtractor');

// Queue
const Producer = require('./src/queue/Producer');
const Consumer = require('./src/queue/Consumer');

// Validation & Transform
const RuleEngine = require('./src/validation/RuleEngine');
const TransformEngine = require('./src/transform/TransformEngine');

// Deduplication & Load
const DeduplicationService = require('./src/deduplication/DeduplicationService');
const DataWarehouseLoader = require('./src/load/DataWarehouseLoader');

class ETLPipeline {
  constructor() {
    this.postgresExtractor = new PostgresExtractor();
    this.csvExtractor = new CsvExtractor();
    this.producer = new Producer();
    this.consumer = new Consumer();
    this.ruleEngine = new RuleEngine();
    this.transformEngine = new TransformEngine();
    this.deduplicationService = new DeduplicationService();
    this.dataWarehouseLoader = new DataWarehouseLoader();
  }

  /**
   * Chạy toàn bộ ETL Pipeline
   */
  async run() {
    try {
      await logger.startPhase('ETL PIPELINE STARTED');
      const startTime = Date.now();

      // ============================================
      // PHASE 1: EXTRACT
      // ============================================
      await this.extractPhase();

      // ============================================
      // PHASE 2: QUEUE & STAGING
      // ============================================
      await this.queuePhase();

      // ============================================
      // PHASE 3: DATA QUALITY
      // ============================================
      await this.dataQualityPhase();

      // ============================================
      // PHASE 4: LOAD
      // ============================================
      await this.loadPhase();

      const duration = (Date.now() - startTime) / 1000;
      await logger.endPhase('ETL PIPELINE COMPLETED', {
        duration: `${duration}s`,
        status: 'SUCCESS'
      });

    } catch (error) {
      await logger.error('ETL Pipeline failed', error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  /**
   * PHASE 1: Extract dữ liệu từ 2 data sources
   */
  async extractPhase() {
    await logger.startPhase('PHASE 1: EXTRACT');

    // Extract từ PostgreSQL (Data Source 1)
    const postgresData = await this.postgresExtractor.extractAll();
    await logger.success('✓ Extracted PostgreSQL data', {
      tables: Object.keys(postgresData).length,
      totalRecords: this.countRecords(postgresData)
    });

    // Validate CSV files
    await this.csvExtractor.validateFiles();

    // Extract từ CSV (Data Source 2)
    const csvData = await this.csvExtractor.extractAll();
    await logger.success('✓ Extracted CSV data', {
      files: Object.keys(csvData).length,
      totalRecords: this.countRecords(csvData)
    });

    this.extractedData = {
      postgres: postgresData,
      csv: csvData
    };

    await logger.endPhase('PHASE 1: EXTRACT');
  }

  /**
   * PHASE 2: Queue dữ liệu qua RabbitMQ và đưa vào Staging
   */
  async queuePhase() {
    await logger.startPhase('PHASE 2: QUEUE & STAGING');

    // Kết nối RabbitMQ
    await rabbitMQ.connect();
    await this.producer.initialize();
    await this.consumer.initialize();

    // Producer: Đẩy dữ liệu vào queues
    await logger.info('Sending data to queues...');
    await this.producer.sendPostgresData(this.extractedData.postgres);
    await this.producer.sendCsvData(this.extractedData.csv);

    // Consumer: Nhận dữ liệu và lưu vào Staging
    await logger.info('Consuming data from queues...');
    await this.consumer.consumeAll();

    await logger.endPhase('PHASE 2: QUEUE & STAGING');
  }

  /**
   * PHASE 3: Data Quality - Validation, Deduplication, Transform
   */
  async dataQualityPhase() {
    await logger.startPhase('PHASE 3: DATA QUALITY');

    // Lấy dữ liệu từ Staging
    const stagingData = await this.consumer.stagingService.getAllData();

    // Sub-phase 3.1: Deduplication
    await logger.info('Starting deduplication...');
    const deduplicatedData = await this.deduplicationService.removeDuplicates(stagingData);
    await logger.success('✓ Deduplication completed', {
      originalRecords: this.countRecords(stagingData),
      afterDedup: this.countRecords(deduplicatedData),
      removed: this.countRecords(stagingData) - this.countRecords(deduplicatedData)
    });

    // Sub-phase 3.2: Validation
    await logger.info('Starting validation...');
    const validationResults = {};
    for (const [tableName, records] of Object.entries(deduplicatedData)) {
      const result = this.ruleEngine.validateBatch(records);
      validationResults[tableName] = result;
      
      await logger.info(`Validated ${tableName}`, {
        total: result.totalRecords,
        valid: result.validRecords,
        fixable: result.fixableRecords,
        unfixable: result.unfixableRecords
      });
    }

    // Sub-phase 3.3: Transform (fix fixable errors)
    await logger.info('Starting transformation...');
    const transformedData = {};
    for (const [tableName, records] of Object.entries(deduplicatedData)) {
      const validationResult = validationResults[tableName];
      const transformResult = await this.transformEngine.transformBatch(
        records,
        validationResult
      );
      transformedData[tableName] = transformResult.records;
      
      await logger.success(`✓ Transformed ${tableName}`, {
        transformed: transformResult.transformedRecords,
        skipped: transformResult.skippedRecords
      });
    }

    // Sub-phase 3.4: Re-validate sau transform
    await logger.info('Re-validating after transformation...');
    const finalValidationResults = {};
    const cleanData = {};
    const errorData = {};

    for (const [tableName, records] of Object.entries(transformedData)) {
      const result = this.ruleEngine.validateBatch(records);
      finalValidationResults[tableName] = result;

      // Tách clean data và error data
      cleanData[tableName] = result.results
        .filter(r => r.isValid)
        .map(r => r.record);
      
      errorData[tableName] = result.results
        .filter(r => !r.isValid)
        .map(r => ({ record: r.record, errors: r.errors }));

      await logger.info(`Final validation for ${tableName}`, {
        clean: cleanData[tableName].length,
        stillInvalid: errorData[tableName].length
      });
    }

    // Log errors
    for (const [tableName, errors] of Object.entries(errorData)) {
      if (errors.length > 0) {
        await logger.error(`Unfixable errors in ${tableName}`, null, {
          count: errors.length,
          samples: errors.slice(0, 3)
        });
      }
    }

    this.cleanData = cleanData;
    this.errorData = errorData;

    await logger.endPhase('PHASE 3: DATA QUALITY');
  }

  /**
   * PHASE 4: Load clean data vào Data Warehouse
   */
  async loadPhase() {
    await logger.startPhase('PHASE 4: LOAD TO DATA WAREHOUSE');

    await this.dataWarehouseLoader.initialize();
    
    // Load clean data
    const loadResult = await this.dataWarehouseLoader.loadAll(this.cleanData);
    
    await logger.success('✓ Data loaded to warehouse', loadResult);

    await logger.endPhase('PHASE 4: LOAD TO DATA WAREHOUSE');
  }

  /**
   * Cleanup resources
   */
  async cleanup() {
    await logger.info('Cleaning up resources...');
    await rabbitMQ.close();
    await dbManager.closeAll();
    await logger.success('✓ Cleanup completed');
  }

  /**
   * Helper: Đếm tổng records
   */
  countRecords(data) {
    return Object.values(data).reduce((sum, records) => sum + records.length, 0);
  }
}

// ============================================
// MAIN EXECUTION
// ============================================
async function main() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║   ETL DATA INTEGRATION PIPELINE                           ║');
  console.log('║   E-commerce & Customer Care System                       ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');

  const pipeline = new ETLPipeline();
  
  try {
    await pipeline.run();
    console.log('');
    console.log('✅ ETL Pipeline completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('');
    console.error('❌ ETL Pipeline failed!');
    console.error(error);
    process.exit(1);
  }
}

// Run if this is the main module
if (require.main === module) {
  main();
}

module.exports = ETLPipeline;