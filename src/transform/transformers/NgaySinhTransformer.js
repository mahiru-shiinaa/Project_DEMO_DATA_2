// src/transform/transformers/NgaySinhTransformer.js
// ✅ Transformer riêng cho field ngay_sinh
const moment = require("moment");

class NgaySinhTransformer {
  constructor() {
    this.fieldName = "ngay_sinh";
    this.defaultDate = "2004-05-29"; // Ngày sinh mặc định
  }

  /**
   * Transform ngay_sinh với logic cụ thể:
   * 1. Null/undefined/empty -> gán default date (2004-05-29)
   * 2. Parse với nhiều format khác nhau
   * 3. Validate tuổi hợp lệ (13-120)
   * 4. Format về YYYY-MM-DD
   */
  transform(value, record, validationErrors = []) {
    // ✅ Case 1: Null/undefined/empty -> default date
    if (!value || value === '' || value === null || value === undefined) {
      console.log(`📅 ngay_sinh is null for ${record.ma_khach_hang || 'unknown'}, setting default: ${this.defaultDate}`);
      return this.defaultDate;
    }

    // ✅ Case 2: Nếu đã đúng format YYYY-MM-DD và hợp lệ -> giữ nguyên
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
      const date = moment(value, 'YYYY-MM-DD', true);
      if (date.isValid() && this.isValidAge(date)) {
        return value;
      }
    }

    // ✅ Case 3: Parse với nhiều format phổ biến
    const formats = [
      'YYYY-MM-DD',
      'DD/MM/YYYY',
      'MM/DD/YYYY',
      'DD-MM-YYYY',
      'YYYY/MM/DD',
      'YYYYMMDD',
      moment.ISO_8601
    ];

    let parsedDate = null;
    for (const format of formats) {
      const date = moment(value, format, true);
      if (date.isValid()) {
        parsedDate = date;
        break;
      }
    }

    // ✅ Case 4: Parse thành công
    if (parsedDate && parsedDate.isValid()) {
      // Kiểm tra tuổi hợp lệ
      if (!this.isValidAge(parsedDate)) {
        console.warn(`⚠️ Invalid age for ${value}, using default: ${this.defaultDate}`);
        return this.defaultDate;
      }

      // Kiểm tra không phải ngày tương lai
      if (parsedDate.isAfter(moment())) {
        console.warn(`⚠️ Future date ${value}, using default: ${this.defaultDate}`);
        return this.defaultDate;
      }

      return parsedDate.format('YYYY-MM-DD');
    }

    // ✅ Case 5: Parse thất bại -> default
    console.warn(`⚠️ Cannot parse date: ${value}, using default: ${this.defaultDate}`);
    return this.defaultDate;
  }

  /**
   * Kiểm tra tuổi có hợp lệ không (13-120 tuổi)
   */
  isValidAge(date) {
    const age = moment().diff(date, 'years');
    return age >= 13 && age <= 120 && date.year() >= 1900;
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
        }
      }
      
      return transformed;
    });
  }

  /**
   * Kiểm tra xem record có field này không
   */
  shouldTransform(record) {
    return record.hasOwnProperty(this.fieldName);
  }

  /**
   * Check xem có thể transform không
   */
  canTransform(value, validationErrors) {
    // Luôn có thể transform vì có default value
    return true;
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue || 'null',
      transformed: transformedValue,
      action: !originalValue 
        ? "set_default_date" 
        : "format_and_validate_date",
      note: !originalValue 
        ? `Default date: ${this.defaultDate}` 
        : `Parsed and formatted`
    };
  }
}

module.exports = NgaySinhTransformer;