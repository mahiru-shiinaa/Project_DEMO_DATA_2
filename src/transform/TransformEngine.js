// src/transform/TransformEngine.js - FIXED VERSION
const logger = require('../utils/Logger');
const fs = require('fs');
const path = require('path');

class TransformEngine {
  constructor() {
    this.transformers = new Map();
    this.loadAllTransformers();
  }

  loadAllTransformers() {
    const transformersDir = path.join(__dirname, 'transformers');
    const transformerFiles = fs.readdirSync(transformersDir)
      .filter(f => f.endsWith('Transformer.js') && f !== 'IdTransformer.js'); // ✅ Skip IdTransformer

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

    logger.info(`Loaded ${this.transformers.size} transformers (excluding IdTransformer)`);
  }

  /**
   * ✅ Transform record KHÔNG BAO GỒM ID transform (đã chạy rồi)
   */
  transformRecord(record, validationResult) {
    let transformed = { ...record };
    const transformLog = [];

    // Bước 1: Áp dụng transform chủ động cho loai_khach_hang
    if (this.transformers.has('loai_khach_hang') && transformed.hasOwnProperty('ngay_dang_ky')) {
      const transformer = this.transformers.get('loai_khach_hang');
      const originalValue = transformed.loai_khach_hang;
      const transformedValue = transformer.transform(originalValue, transformed);

      if (transformedValue !== originalValue) {
        transformed.loai_khach_hang = transformedValue;
        transformLog.push(transformer.logTransform(originalValue, transformedValue));
      }
    }

    // Bước 2: Áp dụng transform dựa trên lỗi validation
    if (validationResult && validationResult.fixableErrors.length > 0) {
      const fixableFields = new Set(validationResult.fixableErrors.map(e => e.field));

      for (const fieldName of fixableFields) {
        if (this.transformers.has(fieldName)) {
          const transformer = this.transformers.get(fieldName);
          const originalValue = transformed[fieldName];
          const fieldErrors = validationResult.errors.filter(e => e.field === fieldName);
          const transformedValue = transformer.transform(originalValue, transformed, fieldErrors);

          if (transformedValue !== originalValue) {
            transformed[fieldName] = transformedValue;
            if (!transformLog.some(log => log.field === fieldName)) {
              transformLog.push(transformer.logTransform(originalValue, transformedValue));
            }
          }
        }
      }
    }

    return {
      record: transformed,
      transformed: transformLog.length > 0,
      log: transformLog,
    };
  }

  /**
   * ✅ Transform batch KHÔNG CÓ ID transform
   */
  async transformBatchWithoutId(records, validationResults) {
    await logger.startPhase('TRANSFORM - Applying Field Transformations (No ID)');

    const results = {
      totalRecords: records.length,
      transformedRecords: 0,
      skippedRecords: 0,
      records: [],
      logs: []
    };

    for (let i = 0; i < records.length; i++) {
      const originalRecord = records[i];
      const validationResult = validationResults.results[i];

      // Chỉ transform các field không phải ID
      const fieldTransformResult = this.transformRecord(originalRecord, validationResult);
      
      const finalRecord = fieldTransformResult.record;
      results.records.push(finalRecord);
      
      if (JSON.stringify(originalRecord) !== JSON.stringify(finalRecord)) {
        results.transformedRecords++;
        results.logs.push({
          recordIndex: i,
          originalRecord: originalRecord,
          transformedRecord: finalRecord,
          transformLog: fieldTransformResult.log
        });
      } else {
        results.skippedRecords++;
      }
    }

    await logger.endPhase('TRANSFORM - Applying Field Transformations (No ID)', {
      totalRecords: results.totalRecords,
      transformedRecords: results.transformedRecords,
      skippedRecords: results.skippedRecords
    });

    if (results.logs.length > 0) {
      await logger.info('Transform details', {
        sampleTransforms: results.logs.slice(0, 5)
      });
    }

    return results;
  }

  /**
   * ✅ Transform batch GỐC (bao gồm ID transform) - giữ lại cho backward compatibility
   */
  async transformBatch(records, validationResults) {
    // Deprecated - nên dùng transformBatchWithoutId
    return this.transformBatchWithoutId(records, validationResults);
  }

  // Các helper methods giữ nguyên
  transformWithStrategy(record, fieldName, strategyFn) {
    const transformed = { ...record };
    const originalValue = record[fieldName];
    transformed[fieldName] = strategyFn(originalValue, record);
    return {
      record: transformed,
      changed: transformed[fieldName] !== originalValue
    };
  }

  addTransformer(fieldName, transformerInstance) {
    this.transformers.set(fieldName, transformerInstance);
    logger.info(`Added transformer for field: ${fieldName}`);
  }

  getTransformer(fieldName) {
    return this.transformers.get(fieldName);
  }

  hasTransformer(fieldName) {
    return this.transformers.has(fieldName);
  }

  getAvailableTransformers() {
    return Array.from(this.transformers.keys());
  }

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
