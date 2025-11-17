// src/validation/rules/DonGia.rule.js
const { LIMITS, ERROR_CODES } = require('../../../config/constants');

class DonGiaRule {
  constructor() {
    this.fieldName = 'don_gia';
  }

  validate(value, record) {
    const errors = [];
    if (value === null || value === undefined) {
      return { isValid: true, errors: [] }; // Cho phép null
    }

    const price = parseFloat(value);
    if (isNaN(price)) {
      errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Đơn giá phải là một con số.',
        value,
        canFix: false,
      });
    }

    if (price < LIMITS.MIN_PRICE || price > LIMITS.MAX_PRICE) {
      errors.push({
        code: ERROR_CODES.OUT_OF_RANGE,
        field: this.fieldName,
        message: `Đơn giá phải nằm trong khoảng từ ${LIMITS.MIN_PRICE} đến ${LIMITS.MAX_PRICE}.`,
        value,
        canFix: false,
      });
    }

    return {
      isValid: errors.length === 0,
      canFix: false,
      errors: errors,
    };
  }

  getFieldName() {
    return this.fieldName;
  }
}

module.exports = DonGiaRule;