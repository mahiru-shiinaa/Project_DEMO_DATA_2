// src/transform/transformers/TrangThaiTransformer.js
class TrangThaiTransformer {
  constructor() {
    this.fieldName = 'trang_thai';
  }

  transform(value, record, validationErrors = []) {
    if (!value) return value;

    // Lấy suggestion từ lỗi validation nếu có (Module TrangThai.rule.js đã gợi ý sửa lỗi chính tả)
    const error = validationErrors.find(e => e.suggestion && e.field === this.fieldName);
    if (error && error.suggestion) {
        // Giả sử suggestion dạng: "Có thể là: Đã giao" hoặc "Chuyển thành: Đã giao"
        const match = error.suggestion.split(': ');
        if (match.length > 1) {
            return match[1].trim();
        }
    }

    // Mặc định viết hoa chữ cái đầu
    return value.charAt(0).toUpperCase() + value.slice(1);
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName,
      original: originalValue,
      transformed: transformedValue,
      action: 'normalize_status'
    };
  }
}

module.exports = TrangThaiTransformer;