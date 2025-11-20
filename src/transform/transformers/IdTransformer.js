// src/transform/transformers/IdTransformer.js
// ✅ Giải quyết vấn đề KEY COLLISION giữa 2 data sources

class IdTransformer {
  constructor() {
    this.fieldName = 'id'; // Generic, sẽ apply cho nhiều fields
    
    // Danh sách các ID fields cần prefix
    this.idFields = [
      'ma_khach_hang',
      'ma_don_hang',
      'ma_san_pham',
      'ma_danh_muc',
      'ma_thanh_toan',
      'ma_phieu_ho_tro',
      'ma_danh_gia',
      'ma_nhan_vien',
      'ma_phieu_xu_ly'
    ];

    // Mapping source -> prefix
    this.prefixes = {
      'postgresql': 'PG_',
      'csv': 'CSV_'
    };
  }

  /**
   * Transform ALL ID fields trong một record
   * @param {object} record - Record cần transform
   * @returns {object} - Transformed record
   */
  transformRecord(record) {
    if (!record || !record.source) {
      return record;
    }

    const prefix = this.prefixes[record.source];
    if (!prefix) {
      return record; // Unknown source, skip
    }

    const transformed = { ...record };

    // Apply prefix cho TẤT CẢ ID fields
    for (const field of this.idFields) {
      if (transformed[field]) {
        const originalValue = transformed[field];
        
        // Chỉ prefix nếu chưa có prefix
        if (!originalValue.startsWith('PG_') && !originalValue.startsWith('CSV_')) {
          transformed[field] = prefix + originalValue;
        }
      }
    }

    return transformed;
  }

  /**
   * Transform batch records
   */
  transformBatch(records) {
    return records.map(record => this.transformRecord(record));
  }

  /**
   * Check xem field có phải là ID field không
   */
  isIdField(fieldName) {
    return this.idFields.includes(fieldName);
  }

  /**
   * Get prefix cho một source
   */
  getPrefixForSource(source) {
    return this.prefixes[source] || '';
  }

  /**
   * Remove prefix (nếu cần revert)
   */
  removePrefix(value) {
    if (typeof value !== 'string') return value;
    
    for (const prefix of Object.values(this.prefixes)) {
      if (value.startsWith(prefix)) {
        return value.substring(prefix.length);
      }
    }
    
    return value;
  }

  getFieldName() {
    return this.fieldName;
  }

  logTransform(originalValue, transformedValue) {
    return {
      field: 'id_fields',
      original: originalValue,
      transformed: transformedValue,
      action: 'add_source_prefix'
    };
  }
}

module.exports = IdTransformer;