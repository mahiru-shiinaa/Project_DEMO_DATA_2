// src/transform/transformers/LoaiKhachHangTransformer.js
// ✅ Xử lý default value cho loai_khach_hang

const { LOAI_KHACH_HANG } = require('../../../config/constants');

class LoaiKhachHangTransformer {
  constructor() {
    this.fieldName = 'loai_khach_hang';
    this.defaultValue = LOAI_KHACH_HANG.THUONG; // "Thường"
  }

  /**
   * Transform loai_khach_hang:
   * - Nếu null/undefined/empty -> set mặc định "Thường"
   * - Nếu có giá trị -> chuẩn hóa
   */
  transform(value, record, validationErrors = []) {
    // Case 1: Null/undefined/empty -> Default
    if (!value || value.toString().trim() === '') {
      return this.defaultValue;
    }

    // Case 2: Chuẩn hóa giá trị hiện có
    const normalized = value.toString().trim();
    
    // Kiểm tra xem có thuộc enum không
    const validTypes = Object.values(LOAI_KHACH_HANG);
    const matchedType = validTypes.find(
      type => type.toLowerCase() === normalized.toLowerCase()
    );

    if (matchedType) {
      return matchedType; // Trả về giá trị chuẩn từ enum
    }

    // Case 3: Không match -> vẫn trả default
    return this.defaultValue;
  }

  /**
   * Transform batch
   */
  transformBatch(records) {
    return records.map(record => {
      const transformed = { ...record };
      
      // Chỉ transform nếu record thuộc table khach_hang
      if (this.shouldTransform(record)) {
        transformed[this.fieldName] = this.transform(
          record[this.fieldName],
          record
        );
      }
      
      return transformed;
    });
  }

  /**
   * Kiểm tra xem có nên transform không
   */
  shouldTransform(record) {
    // Chỉ transform cho records của khach_hang
    return record.ma_khach_hang !== undefined;
  }

  /**
   * Validate trước khi transform
   */
  canTransform(value, record) {
    return this.shouldTransform(record);
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue || 'null',
      transformed: transformedValue,
      action: originalValue ? 'normalize' : 'set_default_value'
    };
  }
}

module.exports = LoaiKhachHangTransformer;