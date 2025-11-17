// src/transform/transformers/EmailTransformer.js - Transform Email
class EmailTransformer {
  constructor() {
    this.fieldName = 'email';
  }

  /**
   * Transform email theo các quy tắc:
   * 1. Chuyển tất cả thành lowercase
   * 2. Xóa khoảng trắng
   * 3. Trim đầu cuối
   */
  transform(value, record, validationErrors = []) {
    if (!value) return null;

    let transformed = value.toString();

    // Step 1: Trim
    transformed = transformed.trim();

    // Step 2: Xóa tất cả khoảng trắng
    transformed = transformed.replace(/\s+/g, '');

    // Step 3: Chuyển thành lowercase
    transformed = transformed.toLowerCase();

    // Step 4: Fix common typos
    transformed = this.fixCommonTypos(transformed);

    return transformed;
  }

  /**
   * Sửa các lỗi typo phổ biến
   */
  fixCommonTypos(email) {
    // Replace common typos
    const typos = {
      'gmial.com': 'gmail.com',
      'gmai.com': 'gmail.com',
      'yahooo.com': 'yahoo.com',
      'yaho.com': 'yahoo.com',
      'hotmial.com': 'hotmail.com',
      'outloook.com': 'outlook.com'
    };

    let fixed = email;
    for (const [typo, correct] of Object.entries(typos)) {
      if (fixed.includes(typo)) {
        fixed = fixed.replace(typo, correct);
      }
    }

    return fixed;
  }

  canTransform(value, validationErrors) {
    return validationErrors && validationErrors.some(e => e.canFix);
  }

  getFieldName() {
    return this.fieldName;
  }

  transformBatch(records, errorsByRecord) {
    return records.map((record, index) => {
      const errors = errorsByRecord[index] || [];
      const transformed = { ...record };
      
      if (record[this.fieldName]) {
        transformed[this.fieldName] = this.transform(
          record[this.fieldName],
          record,
          errors
        );
      }
      
      return transformed;
    });
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue,
      transformed: transformedValue,
      action: 'lowercase_and_normalize'
    };
  }
}

module.exports = EmailTransformer;