// src/transform/transformers/NgayThangTransformer.js
const moment = require('moment');

class NgayThangTransformer {
  constructor() {
    // Sẽ áp dụng cho tất cả các trường có tên là 'ngay_sinh', 'ngay_dat', etc.
    // Nhưng để đơn giản, ta chỉ định một field chính. TransformEngine sẽ tìm đúng transformer.
    this.fieldName = 'ngay_sinh'; 
  }

  transform(value, record, validationErrors = []) {
    if (!value) return null;

    // Thử parse date với các định dạng phổ biến
    const date = moment(value);
    
    if (date.isValid()) {
      // Trả về định dạng chuẩn YYYY-MM-DD
      return date.format('YYYY-MM-DD');
    }

    // Nếu không parse được, trả về giá trị gốc để không làm mất dữ liệu
    return value;
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: this.fieldName, // Field thực tế có thể khác, nhưng log này là đủ
      original: originalValue,
      transformed: transformedValue,
      action: 'format_date_to_yyyy_mm_dd'
    };
  }
}

module.exports = NgayThangTransformer;