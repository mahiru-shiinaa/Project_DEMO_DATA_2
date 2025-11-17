// src/validation/rules/Email.rule.js - Validation cho Email
const { REGEX, LIMITS, ERROR_CODES } = require('../../../config/constants');

class EmailRule {
  constructor() {
    this.fieldName = 'email';
    this.errors = [];
  }

  validate(value, record) {
    this.errors = [];
    let canFix = true;

    // Rule 1: Không được null
    if (!value || value.toString().trim() === '') {
      this.errors.push({
        code: ERROR_CODES.NULL_VALUE,
        field: this.fieldName,
        message: 'Email không được để trống',
        value: value,
        canFix: false
      });
      return {
        isValid: false,
        canFix: false,
        errors: this.errors,
        value: value
      };
    }

    value = value.toString().trim().toLowerCase();

    // Rule 2: Kiểm tra format email với Regex
    if (!REGEX.EMAIL.test(value)) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Email không đúng định dạng',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 3: Độ dài không vượt quá
    if (value.length > LIMITS.MAX_EMAIL_LENGTH) {
      this.errors.push({
        code: ERROR_CODES.OUT_OF_RANGE,
        field: this.fieldName,
        message: `Email không được vượt quá ${LIMITS.MAX_EMAIL_LENGTH} ký tự`,
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 4: Không chứa khoảng trắng (có thể fix)
    if (/\s/.test(value)) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Email không được chứa khoảng trắng',
        value: value,
        canFix: true,
        suggestion: 'Xóa tất cả khoảng trắng'
      });
    }

    // Rule 5: Phải có @ và domain hợp lệ
    if (value.includes('@')) {
      const parts = value.split('@');
      if (parts.length !== 2 || parts[0].length === 0 || parts[1].length === 0) {
        this.errors.push({
          code: ERROR_CODES.INVALID_FORMAT,
          field: this.fieldName,
          message: 'Email thiếu phần trước hoặc sau @',
          value: value,
          canFix: false
        });
        canFix = false;
      }

      // Kiểm tra domain
      if (parts[1] && !parts[1].includes('.')) {
        this.errors.push({
          code: ERROR_CODES.INVALID_FORMAT,
          field: this.fieldName,
          message: 'Domain email không hợp lệ (thiếu .)',
          value: value,
          canFix: false
        });
        canFix = false;
      }
    }

    // Rule 6: Không được có ký tự đặc biệt không hợp lệ
    const invalidChars = /[<>()[\]\\,;:"]/.test(value);
    if (invalidChars) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Email chứa ký tự đặc biệt không hợp lệ',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 7: Email phải là chữ thường (có thể fix)
    if (value !== value.toLowerCase()) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Email nên được viết thường',
        value: value,
        canFix: true,
        suggestion: 'Chuyển tất cả thành chữ thường'
      });
    }

    const isValid = this.errors.length === 0;

    return {
      isValid,
      canFix: canFix && this.errors.length > 0,
      errors: this.errors,
      value: value,
      needsTransform: this.errors.some(e => e.canFix)
    };
  }

  getErrors() {
    return this.errors;
  }

  getFieldName() {
    return this.fieldName;
  }
}

module.exports = EmailRule;