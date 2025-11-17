// src/validation/rules/SoDienThoai.rule.js
const { REGEX, ERROR_CODES } = require('../../../config/constants');

class SoDienThoaiRule {
  constructor() {
    this.fieldName = 'so_dien_thoai';
  }

  validate(value, record) {
    const errors = [];
    if (!value) {
      return { isValid: true, errors: [] }; // Cho phép null
    }

    if (!REGEX.PHONE.test(value.toString())) {
      errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Số điện thoại không hợp lệ. Phải bắt đầu bằng 0 hoặc +84 và có 9-10 số theo sau.',
        value,
        canFix: true, // Có thể sửa được
        suggestion: 'Chuẩn hóa về định dạng 0xxxxxxxxx'
      });
    }

    return {
      isValid: errors.length === 0,
      canFix: errors.length > 0,
      errors: errors,
    };
  }

  getFieldName() {
    return this.fieldName;
  }
}

module.exports = SoDienThoaiRule;