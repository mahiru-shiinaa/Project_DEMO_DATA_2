// src/validation/rules/TrangThai.rule.js - Validation cho Trạng thái (Enum)
const { 
  TRANG_THAI_DON_HANG, 
  TRANG_THAI_THANH_TOAN,
  TRANG_THAI_PHIEU_HO_TRO,
  TRANG_THAI_NHAN_VIEN,
  ERROR_CODES 
} = require('../../../config/constants');

class TrangThaiRule {
  constructor() {
    this.fieldName = 'trang_thai';
    this.errors = [];
    
    // Map các bảng với enum tương ứng
    this.statusMaps = {
      'don_hang': Object.values(TRANG_THAI_DON_HANG),
      'thanh_toan': Object.values(TRANG_THAI_THANH_TOAN),
      'phieu_ho_tro': Object.values(TRANG_THAI_PHIEU_HO_TRO),
      'nhanvien_cskh': Object.values(TRANG_THAI_NHAN_VIEN)
    };
  }

  /**
   * Validate trạng thái dựa vào table context
   */
  validate(value, record, tableName = null) {
    this.errors = [];

    // Rule 1: Không được null
    if (!value || value.toString().trim() === '') {
      this.errors.push({
        code: ERROR_CODES.NULL_VALUE,
        field: this.fieldName,
        message: 'Trạng thái không được để trống',
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

    // Xác định valid statuses dựa vào table
    const validStatuses = this.getValidStatuses(tableName, record);

    if (!validStatuses || validStatuses.length === 0) {
      // Không xác định được table, skip validation
      return {
        isValid: true,
        canFix: false,
        errors: [],
        value: value
      };
    }

    // Rule 2: Phải thuộc một trong các giá trị hợp lệ
    const isValidStatus = validStatuses.some(status => 
      status.toLowerCase() === value.toLowerCase()
    );

    if (!isValidStatus) {
      // Tìm trạng thái gần giống nhất (có thể fix)
      const closestMatch = this.findClosestMatch(value, validStatuses);
      
      this.errors.push({
        code: ERROR_CODES.INVALID_STATUS,
        field: this.fieldName,
        message: 'Trạng thái không hợp lệ',
        value: value,
        validStatuses: validStatuses,
        canFix: closestMatch !== null,
        suggestion: closestMatch ? `Có thể là: ${closestMatch}` : null
      });
    }

    // Rule 3: Chuẩn hóa chữ hoa/thường (có thể fix)
    const correctStatus = validStatuses.find(status => 
      status.toLowerCase() === value.toLowerCase()
    );

    if (correctStatus && correctStatus !== value) {
      this.errors.push({
        code: ERROR_CODES.INVALID_FORMAT,
        field: this.fieldName,
        message: 'Trạng thái cần chuẩn hóa chữ hoa/thường',
        value: value,
        canFix: true,
        suggestion: `Chuyển thành: ${correctStatus}`
      });
    }

    const isValid = this.errors.length === 0;
    const canFix = !isValid && this.errors.every(e => e.canFix);

    return {
      isValid,
      canFix,
      errors: this.errors,
      value: value,
      needsTransform: canFix
    };
  }

  /**
   * Xác định valid statuses dựa vào table name hoặc record context
   */
  getValidStatuses(tableName, record) {
    // Nếu có tableName
    if (tableName && this.statusMaps[tableName]) {
      return this.statusMaps[tableName];
    }

    // Thử xác định từ record (có thể có field indicator)
    if (record) {
      if (record.ma_don_hang) return this.statusMaps.don_hang;
      if (record.ma_thanh_toan) return this.statusMaps.thanh_toan;
      if (record.ma_phieu_ho_tro) return this.statusMaps.phieu_ho_tro;
      if (record.ma_nhan_vien) return this.statusMaps.nhanvien_cskh;
    }

    // Merge tất cả (fallback)
    return [
      ...this.statusMaps.don_hang,
      ...this.statusMaps.thanh_toan,
      ...this.statusMaps.phieu_ho_tro,
      ...this.statusMaps.nhanvien_cskh
    ];
  }

  /**
   * Tìm trạng thái gần giống nhất (typo correction)
   */
  findClosestMatch(value, validStatuses) {
    const valueLower = value.toLowerCase().replace(/\s+/g, '');
    
    let closestMatch = null;
    let minDistance = Infinity;

    for (const status of validStatuses) {
      const statusLower = status.toLowerCase().replace(/\s+/g, '');
      const distance = this.levenshteinDistance(valueLower, statusLower);
      
      // Nếu độ tương đồng > 70%
      if (distance < minDistance && distance <= statusLower.length * 0.3) {
        minDistance = distance;
        closestMatch = status;
      }
    }

    return closestMatch;
  }

  /**
   * Tính khoảng cách Levenshtein (edit distance)
   */
  levenshteinDistance(str1, str2) {
    const len1 = str1.length;
    const len2 = str2.length;
    const matrix = [];

    for (let i = 0; i <= len1; i++) {
      matrix[i] = [i];
    }

    for (let j = 0; j <= len2; j++) {
      matrix[0][j] = j;
    }

    for (let i = 1; i <= len1; i++) {
      for (let j = 1; j <= len2; j++) {
        if (str1[i - 1] === str2[j - 1]) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1,
            matrix[i][j - 1] + 1,
            matrix[i - 1][j] + 1
          );
        }
      }
    }

    return matrix[len1][len2];
  }

  getErrors() {
    return this.errors;
  }

  getFieldName() {
    return this.fieldName;
  }
}

module.exports = TrangThaiRule;