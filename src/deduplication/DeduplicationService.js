// src/deduplication/DeduplicationService.js - FIXED VERSION
const logger = require('../utils/Logger');

class DeduplicationService {
  constructor() {
    // ✅ Định nghĩa primary keys MỚI (sau khi đã có prefix)
    this.primaryKeys = {
      // Đối với khách hàng: dùng EMAIL thay vì mã (vì 2 nguồn có thể trùng mã)
      khach_hang: ['email'], // ✅ ĐÃ SỬA: dùng email làm unique key
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
          after: deduplicated.length,
          duplicateKeys: this.primaryKeys[tableName]
        });
      } else {
        await logger.success(`No duplicates in ${tableName}`);
      }
    }

    await logger.endPhase('DEDUPLICATION', stats);

    return deduplicatedData;
  }

  /**
   * ✅ Loại bỏ trùng lặp cho một table
   */
  deduplicateTable(tableName, records) {
    const primaryKeys = this.primaryKeys[tableName];
    
    if (!primaryKeys) {
      logger.warn(`No primary keys defined for ${tableName}, skipping deduplication`);
      return records;
    }

    const uniqueMap = new Map();

    for (const record of records) {
      // Tạo composite key từ primary keys
      const key = this.createCompositeKey(record, primaryKeys);
      
      if (!uniqueMap.has(key)) {
        uniqueMap.set(key, record);
      } else {
        // ✅ Nếu trùng, merge thông tin (ưu tiên data đầy đủ hơn)
        const existing = uniqueMap.get(key);
        const updated = this.mergeRecords(existing, record);
        uniqueMap.set(key, updated);
        
        // Log để debug
        logger.debug(`Merged duplicate record in ${tableName}`, {
          key,
          existing: existing.ma_khach_hang || existing.ma_don_hang,
          new: record.ma_khach_hang || record.ma_don_hang
        });
      }
    }

    return Array.from(uniqueMap.values());
  }

  /**
   * ✅ Tạo composite key từ nhiều fields
   */
  createCompositeKey(record, keys) {
    return keys
      .map(key => {
        const value = record[key];
        // Lowercase và trim để chuẩn hóa
        return value !== null && value !== undefined 
          ? String(value).toLowerCase().trim() 
          : 'null';
      })
      .join('|');
  }

  /**
   * ✅ Merge 2 records trùng nhau (ưu tiên source, giữ thông tin đầy đủ)
   */
  mergeRecords(existing, newRecord) {
    const merged = { ...existing };

    // Ưu tiên giá trị không null/undefined/empty
    for (const [key, value] of Object.entries(newRecord)) {
      if (value !== null && value !== undefined && value !== '') {
        if (!merged[key] || merged[key] === null || merged[key] === undefined || merged[key] === '') {
          merged[key] = value;
        }
      }
    }

    // ✅ Merge sources array nếu có
    if (existing.source && newRecord.source) {
      const sources = new Set([
        ...(existing.sources || [existing.source]),
        ...(newRecord.sources || [newRecord.source])
      ]);
      merged.sources = Array.from(sources).join(',');
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
        deduplicationKey: this.primaryKeys[tableName],
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
   * ✅ Merge khách hàng từ 2 data sources (DEPRECATED - không cần nữa)
   * Vì giờ dùng email để deduplicate rồi
   */
  mergeKhachHang(ds1Records, ds2Records) {
    const merged = new Map();

    // Add tất cả từ ds1
    for (const record of ds1Records) {
      const key = record.email.toLowerCase().trim();
      merged.set(key, { ...record, sources: ['ds1'] });
    }

    // Merge với ds2
    for (const record of ds2Records) {
      const key = record.email.toLowerCase().trim();
      
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
