// src/validation/rules/HoTen.rule.js - Validation cho trường Họ Tên
const { LIMITS, ERROR_CODES } = require('../../../config/constants');

/**
 * Design Pattern: Chain of Responsibility
 * Mỗi rule là một handler trong chain
 */
class HoTenRule {
  constructor() {
    this.fieldName = 'ho_ten';
    this.errors = [];
  }

  /**
   * Validate họ tên
   * @returns {object} { isValid, canFix, errors, value }
   */
  validate(value, record) {
    this.errors = [];
    let canFix = true;

    // Rule 1: Không được null hoặc rỗng
    if (!value || value.toString().trim() === '') {
      this.errors.push({
        code: ERROR_CODES.NULL_VALUE,
        field: this.fieldName,
        message: 'Họ tên không được để trống',
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

    value = value.toString().trim();

    // Rule 2: Không được chứa số
    const containsNumber = /\d/.test(value);
    if (containsNumber) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Họ tên không được chứa số',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 3: Không được chứa ký tự đặc biệt (trừ khoảng trắng)
    const specialCharsRegex = /[!@#$%^&*()_+=\[\]{};':"\\|,.<>?]/;
    if (specialCharsRegex.test(value)) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Họ tên không được chứa ký tự đặc biệt',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 4: Độ dài không vượt quá giới hạn
    if (value.length > LIMITS.MAX_NAME_LENGTH) {
      this.errors.push({
        code: ERROR_CODES.OUT_OF_RANGE,
        field: this.fieldName,
        message: `Họ tên không được vượt quá ${LIMITS.MAX_NAME_LENGTH} ký tự`,
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 5: Phải có ít nhất 2 từ (có thể fix được)
    const words = value.split(/\s+/).filter(w => w.length > 0);
    if (words.length < 2) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Họ tên phải có ít nhất 2 từ (họ và tên)',
        value: value,
        canFix: true,
        suggestion: 'Thêm "Không rõ" vào cuối nếu chỉ có 1 từ'
      });
      // Vẫn có thể fix được bằng cách thêm một từ mặc định
    }

    // Rule 6: Không được viết tắt không đúng quy tắc (có thể fix)
    // VD: "le t nga" -> có thể fix thành "Lê Thị Nga"
    const hasInvalidAbbreviation = /\b[a-z]\s/.test(value.toLowerCase());
    if (hasInvalidAbbreviation) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Họ tên có viết tắt không đúng quy tắc',
        value: value,
        canFix: true,
        suggestion: 'Chuẩn hóa chữ cái đầu tiên của mỗi từ viết hoa'
      });
    }

    // Rule 7: Không được viết tắt quá 20 ký tự
    if (value.length > 20) {
      const shortWords = words.filter(w => w.length === 1);
      if (shortWords.length > 0) {
        this.errors.push({
          code: ERROR_CODES.INVALID_FORMAT,
          field: this.fieldName,
          message: 'Họ tên quá dài và có viết tắt',
          value: value,
          canFix: false
        });
        canFix = false;
      }
    }

    // Rule 8: Chữ cái đầu mỗi từ phải viết hoa (có thể fix)
    const hasInvalidCapitalization = words.some(word => {
      return word[0] !== word[0].toUpperCase();
    });
    
    if (hasInvalidCapitalization) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Chữ cái đầu của mỗi từ cần viết hoa',
        value: value,
        canFix: true,
        suggestion: 'Chuyển chữ cái đầu mỗi từ thành chữ hoa'
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

  /**
   * Lấy danh sách lỗi
   */
  getErrors() {
    return this.errors;
  }

  /**
   * Lấy tên field
   */
  getFieldName() {
    return this.fieldName;
  }

  /**
   * Kiểm tra xem có thể tự động fix không
   */
  isAutoFixable(validationResult) {
    return validationResult.canFix && 
           validationResult.errors.every(e => e.canFix);
  }
}

module.exports = HoTenRule;