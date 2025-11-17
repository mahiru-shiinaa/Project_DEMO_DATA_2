// src/deduplication/DeduplicationService.js - Loại bỏ dữ liệu trùng lặp
const logger = require('../utils/Logger');

class DeduplicationService {
  constructor() {
    // Định nghĩa primary keys cho từng bảng
    this.primaryKeys = {
      khach_hang: ['ma_khach_hang'],
      danh_muc: ['ma_danh_muc'],
      san_pham: ['ma_san_pham'],
      don_hang: ['ma_don_hang'],
      chi_tiet_don_hang: ['ma_don_hang', 'ma_san_pham'],
      thanh_toan: ['ma_thanh_toan'],
      phieu_ho_tro: ['ma_phieu_ho_tro'],
      danh_gia: ['ma_danh_gia'],
      nhanvien_cskh: ['ma_nhan_vien'],
      phieu_xu_ly: ['ma_phieu_xu_ly']
    };
  }

  /**
   * Loại bỏ trùng lặp cho tất cả tables
   */
  async removeDuplicates(data) {
    await logger.startPhase('DEDUPLICATION');

    const deduplicatedData = {};
    const stats = {
      totalOriginal: 0,
      totalAfter: 0,
      totalRemoved: 0,
      byTable: {}
    };

    for (const [tableName, records] of Object.entries(data)) {
      if (!records || records.length === 0) {
        deduplicatedData[tableName] = [];
        continue;
      }

      const original = records.length;
      const deduplicated = this.deduplicateTable(tableName, records);
      const removed = original - deduplicated.length;

      deduplicatedData[tableName] = deduplicated;

      stats.totalOriginal += original;
      stats.totalAfter += deduplicated.length;
      stats.totalRemoved += removed;
      stats.byTable[tableName] = {
        original,
        after: deduplicated.length,
        removed
      };

      if (removed > 0) {
        await logger.warn(`Removed ${removed} duplicates from ${tableName}`, {
          original,
          after: deduplicated.length
        });
      } else {
        await logger.success(`No duplicates in ${tableName}`);
      }
    }

    await logger.endPhase('DEDUPLICATION', stats);

    return deduplicatedData;
  }

  /**
   * Loại bỏ trùng lặp cho một table
   */
  deduplicateTable(tableName, records) {
    const primaryKeys = this.primaryKeys[tableName];
    
    if (!primaryKeys) {
      logger.warn(`No primary keys defined for ${tableName}, skipping deduplication`);
      return records;
    }

    // Sử dụng Map để track unique records
    const uniqueMap = new Map();

    for (const record of records) {
      // Tạo composite key từ primary keys
      const key = this.createCompositeKey(record, primaryKeys);
      
      if (!uniqueMap.has(key)) {
        uniqueMap.set(key, record);
      } else {
        // Nếu trùng, giữ record mới hơn (có source từ data source nào)
        const existing = uniqueMap.get(key);
        const updated = this.mergeRecords(existing, record);
        uniqueMap.set(key, updated);
      }
    }

    return Array.from(uniqueMap.values());
  }

  /**
   * Tạo composite key từ nhiều fields
   */
  createCompositeKey(record, keys) {
    return keys
      .map(key => {
        const value = record[key];
        return value !== null && value !== undefined ? String(value).toLowerCase() : 'null';
      })
      .join('|');
  }

  /**
   * Merge 2 records trùng nhau (giữ thông tin đầy đủ hơn)
   */
  mergeRecords(existing, newRecord) {
    const merged = { ...existing };

    // Ưu tiên giá trị không null/undefined
    for (const [key, value] of Object.entries(newRecord)) {
      if (value !== null && value !== undefined && value !== '') {
        if (!merged[key] || merged[key] === null || merged[key] === undefined || merged[key] === '') {
          merged[key] = value;
        }
      }
    }

    return merged;
  }

  /**
   * Loại bỏ trùng lặp dựa trên custom fields
   */
  deduplicateByFields(records, fields) {
    const uniqueMap = new Map();

    for (const record of records) {
      const key = this.createCompositeKey(record, fields);
      
      if (!uniqueMap.has(key)) {
        uniqueMap.set(key, record);
      }
    }

    return Array.from(uniqueMap.values());
  }

  /**
   * Tìm duplicates (không xóa, chỉ report)
   */
  findDuplicates(tableName, records) {
    const primaryKeys = this.primaryKeys[tableName];
    
    if (!primaryKeys) {
      return [];
    }

    const keyMap = new Map();
    const duplicates = [];

    for (const record of records) {
      const key = this.createCompositeKey(record, primaryKeys);
      
      if (keyMap.has(key)) {
        duplicates.push({
          key,
          existing: keyMap.get(key),
          duplicate: record
        });
      } else {
        keyMap.set(key, record);
      }
    }

    return duplicates;
  }

  /**
   * Thống kê duplicates
   */
  async analyzeDuplicates(data) {
    const analysis = {};

    for (const [tableName, records] of Object.entries(data)) {
      const duplicates = this.findDuplicates(tableName, records);
      
      analysis[tableName] = {
        totalRecords: records.length,
        duplicateCount: duplicates.length,
        duplicatePercentage: ((duplicates.length / records.length) * 100).toFixed(2) + '%',
        samples: duplicates.slice(0, 3)
      };
    }

    await logger.info('Duplicate Analysis', analysis);
    return analysis;
  }

  /**
   * Loại bỏ records hoàn toàn giống nhau (all fields)
   */
  removeExactDuplicates(records) {
    const uniqueMap = new Map();

    for (const record of records) {
      const key = JSON.stringify(record);
      uniqueMap.set(key, record);
    }

    return Array.from(uniqueMap.values());
  }

  /**
   * Merge khách hàng từ 2 data sources
   * (Khách hàng có thể xuất hiện ở cả 2 nguồn)
   */
  mergeKhachHang(ds1Records, ds2Records) {
    const merged = new Map();

    // Add tất cả từ ds1
    for (const record of ds1Records) {
      const key = this.createCompositeKey(record, ['ma_khach_hang']);
      merged.set(key, { ...record, sources: ['ds1'] });
    }

    // Merge với ds2
    for (const record of ds2Records) {
      const key = this.createCompositeKey(record, ['ma_khach_hang']);
      
      if (merged.has(key)) {
        // Merge thông tin
        const existing = merged.get(key);
        const mergedRecord = this.mergeRecords(existing, record);
        mergedRecord.sources = ['ds1', 'ds2'];
        merged.set(key, mergedRecord);
      } else {
        merged.set(key, { ...record, sources: ['ds2'] });
      }
    }

    return Array.from(merged.values());
  }
}

module.exports = DeduplicationService;