// src/transform/transformers/NgayThangTransformer.js - ENHANCED VERSION
const moment = require("moment");

class NgayThangTransformer {
  constructor() {
    this.fieldName = "ngay_sinh";
    this.defaultDate = "2004-05-29"; // ngày mặc định
  }

  /**
   * Transform ngày sinh:
   * 1. Nếu null/undefined/empty -> gán default date
   * 2. Nếu có giá trị -> parse và format về YYYY-MM-DD
   */
  transform(value, record, validationErrors = []) {
    // ✅ Case 1: Null/undefined/empty -> trả về ngày mặc định
    if (!value || value === '' || value === null || value === undefined) {
      return this.defaultDate;
    }

    // ✅ Case 2: Parse date với nhiều format
    const date = moment(value, [
      'YYYY-MM-DD',
      'DD/MM/YYYY',
      'MM/DD/YYYY',
      'DD-MM-YYYY',
      'YYYY/MM/DD',
      moment.ISO_8601
    ], true);

    if (date.isValid()) {
      return date.format("YYYY-MM-DD");
    }

    // ✅ Case 3: Không parse được -> trả về default
    console.warn(`⚠️ Không parse được ngày sinh: ${value}, sử dụng default: ${this.defaultDate}`);
    return this.defaultDate;
  }

  /**
   * Transform batch records
   */
  transformBatch(records) {
    return records.map(record => {
      const transformed = { ...record };
      
      if (record.hasOwnProperty(this.fieldName)) {
        const originalValue = record[this.fieldName];
        const transformedValue = this.transform(originalValue, record);
        
        if (transformedValue !== originalValue) {
          transformed[this.fieldName] = transformedValue;
          console.log(`🔄 Transformed ${this.fieldName}: ${originalValue || 'null'} -> ${transformedValue}`);
        }
      }
      
      return transformed;
    });
  }

  /**
   * Kiểm tra xem có cần transform không
   */
  shouldTransform(record) {
    return record.hasOwnProperty(this.fieldName);
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue || 'null',
      transformed: transformedValue,
      action: originalValue ? "format_date_to_yyyy_mm_dd" : "set_default_date",
    };
  }
}

module.exports = NgayThangTransformer;