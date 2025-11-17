// src/validation/RuleEngine.js - Design Pattern: Chain of Responsibility
const logger = require('../utils/Logger');
const fs = require('fs');
const path = require('path');

/**
 * RuleEngine áp dụng Chain of Responsibility Pattern
 * Mỗi rule được xử lý tuần tự, nếu fail thì dừng hoặc tiếp tục tùy config
 */
class RuleEngine {
  constructor() {
    this.rules = new Map();
    this.loadAllRules();
  }

  /**
   * Load tất cả các rule files từ thư mục rules/
   */
  loadAllRules() {
    const rulesDir = path.join(__dirname, 'rules');
    const ruleFiles = fs.readdirSync(rulesDir).filter(f => f.endsWith('.rule.js'));

    for (const file of ruleFiles) {
      try {
        const RuleClass = require(path.join(rulesDir, file));
        const ruleInstance = new RuleClass();
        const fieldName = ruleInstance.getFieldName();
        
        this.rules.set(fieldName, ruleInstance);
        console.log(`✓ Loaded rule: ${file} for field: ${fieldName}`);
      } catch (error) {
        console.error(`✗ Failed to load rule: ${file}`, error.message);
      }
    }

    logger.info(`Loaded ${this.rules.size} validation rules`);
  }

  /**
   * Validate một record với tất cả các rules tương ứng
   * @param {object} record - Record cần validate
   * @param {array} fieldsToValidate - Danh sách fields cần validate
   * @returns {object} Kết quả validation
   */
  validateRecord(record, fieldsToValidate = null) {
    const results = {
      isValid: true,
      canFix: true,
      errors: [],
      fixableErrors: [],
      unfixableErrors: [],
      fieldsValidated: []
    };

    // Nếu không chỉ định fields, validate tất cả
    const fields = fieldsToValidate || Object.keys(record);

    for (const field of fields) {
      if (this.rules.has(field)) {
        const rule = this.rules.get(field);
        const value = record[field];
        
        const validationResult = rule.validate(value, record);
        
        results.fieldsValidated.push(field);

        if (!validationResult.isValid) {
          results.isValid = false;
          results.errors.push(...validationResult.errors);

          if (validationResult.canFix) {
            results.fixableErrors.push(...validationResult.errors);
          } else {
            results.unfixableErrors.push(...validationResult.errors);
            results.canFix = false;
          }
        }
      }
    }

    return results;
  }

  /**
   * Validate một batch records
   */
  validateBatch(records, fieldsToValidate = null) {
    const batchResults = {
      totalRecords: records.length,
      validRecords: 0,
      invalidRecords: 0,
      fixableRecords: 0,
      unfixableRecords: 0,
      results: []
    };

    for (const record of records) {
      const result = this.validateRecord(record, fieldsToValidate);
      result.record = record;
      batchResults.results.push(result);

      if (result.isValid) {
        batchResults.validRecords++;
      } else {
        batchResults.invalidRecords++;
        if (result.canFix) {
          batchResults.fixableRecords++;
        } else {
          batchResults.unfixableRecords++;
        }
      }
    }

    return batchResults;
  }

  /**
   * Lấy rule cho một field
   */
  getRule(fieldName) {
    return this.rules.get(fieldName);
  }

  /**
   * Kiểm tra xem có rule cho field không
   */
  hasRule(fieldName) {
    return this.rules.has(fieldName);
  }

  /**
   * Thêm rule mới dynamically
   */
  addRule(fieldName, ruleInstance) {
    this.rules.set(fieldName, ruleInstance);
    logger.info(`Added new rule for field: ${fieldName}`);
  }

  /**
   * Xóa rule
   */
  removeRule(fieldName) {
    this.rules.delete(fieldName);
    logger.info(`Removed rule for field: ${fieldName}`);
  }

  /**
   * Lấy danh sách tất cả các fields có rule
   */
  getAvailableRules() {
    return Array.from(this.rules.keys());
  }

  /**
   * Validate với custom rule chain
   */
  async validateWithChain(record, ruleChain) {
    const results = {
      isValid: true,
      errors: [],
      stoppedAt: null
    };

    for (let i = 0; i < ruleChain.length; i++) {
      const fieldName = ruleChain[i];
      
      if (!this.rules.has(fieldName)) {
        await logger.warn(`No rule found for field: ${fieldName}`);
        continue;
      }

      const rule = this.rules.get(fieldName);
      const value = record[fieldName];
      const validationResult = rule.validate(value, record);

      if (!validationResult.isValid) {
        results.isValid = false;
        results.errors.push(...validationResult.errors);
        
        // Stop chain nếu unfixable
        if (!validationResult.canFix) {
          results.stoppedAt = fieldName;
          break;
        }
      }
    }

    return results;
  }
}

module.exports = RuleEngine;