// src/transform/transformers/HoTenTransformer.js - Transform họ tên
/**
 * Design Pattern: Strategy Pattern
 * Mỗi transformer là một strategy khác nhau để transform data
 */
class HoTenTransformer {
  constructor() {
    this.fieldName = 'ho_ten';
  }

  /**
   * Transform họ tên theo các quy tắc:
   * 1. Trim khoảng trắng thừa
   * 2. Chuẩn hóa chữ cái đầu mỗi từ viết hoa
   * 3. Xử lý viết tắt
   * 4. Chuyển thành tiếng Việt có dấu chuẩn
   */
  transform(value, record, validationErrors = []) {
    if (!value) return null;

    let transformed = value.toString().trim();

    // Step 1: Xóa khoảng trắng thừa
    transformed = this.removeExtraSpaces(transformed);

    // Step 2: Chuẩn hóa viết hoa chữ cái đầu
    transformed = this.capitalizeWords(transformed);

    // Step 3: Xử lý viết tắt (le t nga -> Lê Thị Nga)
    transformed = this.expandAbbreviations(transformed);

    // Step 4: Chuẩn hóa tiếng Việt (nếu có lỗi về encoding)
    transformed = this.normalizeVietnamese(transformed);

    // Step 5: Xử lý trường hợp chỉ có 1 từ
    if (this.shouldAddDefaultSurname(transformed, validationErrors)) {
      transformed = this.addDefaultSurname(transformed);
    }

    return transformed;
  }

  /**
   * Xóa khoảng trắng thừa
   */
  removeExtraSpaces(text) {
    return text.replace(/\s+/g, ' ').trim();
  }

  /**
   * Viết hoa chữ cái đầu mỗi từ
   */
  capitalizeWords(text) {
    return text
      .split(' ')
      .map(word => {
        if (word.length === 0) return word;
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      })
      .join(' ');
  }

  /**
   * Mở rộng viết tắt tiếng Việt
   * VD: "le t nga" -> "Lê Thị Nga"
   */
  expandAbbreviations(text) {
    const words = text.split(' ');
    const expanded = [];

    // Map các viết tắt phổ biến
    const commonAbbreviations = {
      'v': 'Văn',
      't': 'Thị',
      'h': 'Hữu',
      'd': 'Đức',
      'k': 'Kim',
      'm': 'Minh',
      'n': 'Ngọc'
    };

    for (let i = 0; i < words.length; i++) {
      const word = words[i];
      
      // Nếu là viết tắt (1 chữ cái) và ở giữa/cuối
      if (word.length === 1 && i > 0) {
        const abbrev = word.toLowerCase();
        if (commonAbbreviations[abbrev]) {
          expanded.push(commonAbbreviations[abbrev]);
          continue;
        }
      }
      
      expanded.push(word);
    }

    return expanded.join(' ');
  }

  /**
   * Chuẩn hóa tiếng Việt (xử lý encoding sai)
   */
  normalizeVietnamese(text) {
    // Xử lý các trường hợp encoding sai phổ biến
    const replacements = {
      'Ã¡': 'á',
      'Ã ': 'à',
      'áº¡': 'ạ',
      'áº£': 'ả',
      'Ã£': 'ã',
      'Ä': 'đ',
      // ... thêm các mapping khác nếu cần
    };

    let normalized = text;
    for (const [wrong, correct] of Object.entries(replacements)) {
      normalized = normalized.replace(new RegExp(wrong, 'g'), correct);
    }

    return normalized;
  }

  /**
   * Kiểm tra xem có cần thêm họ mặc định không
   */
  shouldAddDefaultSurname(text, validationErrors) {
    const words = text.split(' ');
    return words.length === 1 && 
           validationErrors.some(e => e.message.includes('ít nhất 2 từ'));
  }

  /**
   * Thêm họ mặc định nếu chỉ có tên
   */
  addDefaultSurname(text) {
    // Thêm "Không rõ" vào trước
    return `Không Rõ ${text}`;
  }

  /**
   * Kiểm tra xem transformation có thành công không
   */
  canTransform(value, validationErrors) {
    // Chỉ transform nếu có lỗi có thể fix
    return validationErrors && validationErrors.some(e => e.canFix);
  }

  /**
   * Lấy field name
   */
  getFieldName() {
    return this.fieldName;
  }

  /**
   * Transform batch
   */
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

  /**
   * Logging transform action
   */
  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue,
      transformed: transformedValue,
      action: 'capitalize_and_normalize'
    };
  }
}

module.exports = HoTenTransformer;