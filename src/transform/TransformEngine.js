// src/transform/TransformEngine.js - Design Pattern: Strategy Pattern
const logger = require('../utils/Logger');
const fs = require('fs');
const path = require('path');

/**
 * TransformEngine áp dụng Strategy Pattern
 * Mỗi transformer là một strategy để transform data
 */
class TransformEngine {
  constructor() {
    this.transformers = new Map();
    this.loadAllTransformers();
  }

  /**
   * Load tất cả transformers từ thư mục transformers/
   */
  loadAllTransformers() {
    const transformersDir = path.join(__dirname, 'transformers');
    const transformerFiles = fs.readdirSync(transformersDir)
      .filter(f => f.endsWith('Transformer.js'));

    for (const file of transformerFiles) {
      try {
        const TransformerClass = require(path.join(transformersDir, file));
        const transformerInstance = new TransformerClass();
        const fieldName = transformerInstance.getFieldName();
        
        this.transformers.set(fieldName, transformerInstance);
        console.log(`✓ Loaded transformer: ${file} for field: ${fieldName}`);
      } catch (error) {
        console.error(`✗ Failed to load transformer: ${file}`, error.message);
      }
    }

    logger.info(`Loaded ${this.transformers.size} transformers`);
  }

  /**
   * Transform một record dựa trên validation errors
   * @param {object} record - Record cần transform
   * @param {object} validationResult - Kết quả validation từ RuleEngine
   * @returns {object} Transformed record và metadata
   */
  transformRecord(record, validationResult) {
    const transformed = { ...record };
    const transformLog = [];

    if (!validationResult || validationResult.isValid) {
      return {
        record: transformed,
        transformed: false,
        log: []
      };
    }

    // Chỉ transform các fields có lỗi có thể fix
    const fixableFields = new Set();
    validationResult.fixableErrors.forEach(error => {
      fixableFields.add(error.field);
    });

    // Apply transformers cho từng field
    for (const fieldName of fixableFields) {
      if (this.transformers.has(fieldName)) {
        const transformer = this.transformers.get(fieldName);
        const originalValue = record[fieldName];
        
        // Lấy errors của field này
        const fieldErrors = validationResult.errors.filter(e => e.field === fieldName);
        
        // Transform
        const transformedValue = transformer.transform(
          originalValue,
          record,
          fieldErrors
        );

        if (transformedValue !== originalValue) {
          transformed[fieldName] = transformedValue;
          
          const log = transformer.logTransform(originalValue, transformedValue);
          transformLog.push(log);
        }
      }
    }

    return {
      record: transformed,
      transformed: transformLog.length > 0,
      log: transformLog,
      fieldsTransformed: Array.from(fixableFields)
    };
  }

  /**
   * Transform một batch records
   */
  async transformBatch(records, validationResults) {
    await logger.startPhase('TRANSFORM - Applying Transformations');

    const results = {
      totalRecords: records.length,
      transformedRecords: 0,
      skippedRecords: 0,
      records: [],
      logs: []
    };

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const validationResult = validationResults.results[i];

      const transformResult = this.transformRecord(record, validationResult);
      
      results.records.push(transformResult.record);
      
      if (transformResult.transformed) {
        results.transformedRecords++;
        results.logs.push({
          recordIndex: i,
          originalRecord: record,
          transformedRecord: transformResult.record,
          transformLog: transformResult.log
        });
      } else {
        results.skippedRecords++;
      }
    }

    await logger.endPhase('TRANSFORM - Applying Transformations', {
      totalRecords: results.totalRecords,
      transformedRecords: results.transformedRecords,
      skippedRecords: results.skippedRecords
    });

    // Log chi tiết
    if (results.logs.length > 0) {
      await logger.info('Transform details', {
        sampleTransforms: results.logs.slice(0, 5)
      });
    }

    return results;
  }

  /**
   * Transform với custom strategy
   */
  transformWithStrategy(record, fieldName, strategyFn) {
    const transformed = { ...record };
    const originalValue = record[fieldName];
    
    transformed[fieldName] = strategyFn(originalValue, record);
    
    return {
      record: transformed,
      changed: transformed[fieldName] !== originalValue
    };
  }

  /**
   * Thêm transformer mới dynamically
   */
  addTransformer(fieldName, transformerInstance) {
    this.transformers.set(fieldName, transformerInstance);
    logger.info(`Added transformer for field: ${fieldName}`);
  }

  /**
   * Lấy transformer cho field
   */
  getTransformer(fieldName) {
    return this.transformers.get(fieldName);
  }

  /**
   * Kiểm tra có transformer không
   */
  hasTransformer(fieldName) {
    return this.transformers.has(fieldName);
  }

  /**
   * Lấy danh sách fields có transformer
   */
  getAvailableTransformers() {
    return Array.from(this.transformers.keys());
  }

  /**
   * Apply multiple transforms theo thứ tự
   */
  applyTransformPipeline(record, pipeline) {
    let transformed = { ...record };

    for (const fieldName of pipeline) {
      if (this.transformers.has(fieldName)) {
        const transformer = this.transformers.get(fieldName);
        const value = transformed[fieldName];
        transformed[fieldName] = transformer.transform(value, transformed);
      }
    }

    return transformed;
  }

  /**
   * Transform với conditions
   */
  transformConditional(record, conditions) {
    const transformed = { ...record };

    for (const [fieldName, condition] of Object.entries(conditions)) {
      if (condition(record) && this.transformers.has(fieldName)) {
        const transformer = this.transformers.get(fieldName);
        transformed[fieldName] = transformer.transform(
          record[fieldName],
          record
        );
      }
    }

    return transformed;
  }
}

module.exports = TransformEngine;