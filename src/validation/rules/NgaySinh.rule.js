// src/validation/rules/NgaySinh.rule.js - FIXED VERSION
// ✅ Cho phép transform null values thành default date
const { LIMITS, ERROR_CODES } = require('../../../config/constants');
const moment = require('moment');

class NgaySinhRule {
  constructor() {
    this.fieldName = 'ngay_sinh';
    this.errors = [];
  }

  validate(value, record) {
    this.errors = [];
    let canFix = true;

    // ✅ Rule 1: Nếu null -> CÓ THỂ FIX bằng cách set default date
    if (!value) {
      this.errors.push({
        code: ERROR_CODES.NULL_VALUE,
        field: this.fieldName,
        message: 'Ngày sinh bị null, sẽ được gán giá trị mặc định',
        value: value,
        canFix: true, // ✅ CHO PHÉP FIX
        suggestion: 'Gán ngày sinh mặc định: 2004-05-29'
      });
      return {
        isValid: false,
        canFix: true, // ✅ QUAN TRỌNG: cho phép transform
        errors: this.errors,
        value: value,
        needsTransform: true // ✅ Đánh dấu cần transform
      };
    }

    // Parse date
    let dateValue;
    try {
      dateValue = moment(value);
      if (!dateValue.isValid()) {
        throw new Error('Invalid date');
      }
    } catch (error) {
      this.errors.push({
        code: ERROR_CODES.INVALID_DATE,
        field: this.fieldName,
        message: 'Ngày sinh không đúng định dạng',
        value: value,
        canFix: true,
        suggestion: 'Thử parse với các format khác nhau'
      });
      return {
        isValid: false,
        canFix: true,
        errors: this.errors,
        value: value,
        needsTransform: true
      };
    }

    // Rule 2: Không được là ngày trong tương lai
    if (dateValue.isAfter(moment())) {
      this.errors.push({
        code: ERROR_CODES.INVALID_DATE,
        field: this.fieldName,
        message: 'Ngày sinh không được là ngày trong tương lai',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 3: Tuổi phải từ MIN_AGE đến MAX_AGE
    const age = moment().diff(dateValue, 'years');
    
    if (age < LIMITS.MIN_AGE) {
      this.errors.push({
        code: ERROR_CODES.OUT_OF_RANGE,
        field: this.fieldName,
        message: `Tuổi phải từ ${LIMITS.MIN_AGE} trở lên`,
        value: value,
        currentAge: age,
        canFix: false
      });
      canFix = false;
    }

    if (age > LIMITS.MAX_AGE) {
      this.errors.push({
        code: ERROR_CODES.OUT_OF_RANGE,
        field: this.fieldName,
        message: `Tuổi không được vượt quá ${LIMITS.MAX_AGE}`,
        value: value,
        currentAge: age,
        canFix: false
      });
      canFix = false;
    }

    // Rule 4: Năm sinh phải hợp lý (sau 1900)
    if (dateValue.year() < 1900) {
      this.errors.push({
        code: ERROR_CODES.INVALID_DATE,
        field: this.fieldName,
        message: 'Năm sinh phải sau năm 1900',
        value: value,
        canFix: false
      });
      canFix = false;
    }

    // Rule 5: Kiểm tra format (có thể fix)
    const standardFormat = dateValue.format('YYYY-MM-DD');
    if (value.toString() !== standardFormat) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Định dạng ngày sinh nên là YYYY-MM-DD',
        value: value,
        canFix: true,
        suggestion: `Chuyển thành ${standardFormat}`
      });
    }

    const isValid = this.errors.length === 0;

    return {
      isValid,
      canFix: canFix && this.errors.length > 0,
      errors: this.errors,
      value: value,
      parsedDate: dateValue,
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

module.exports = NgaySinhRule;