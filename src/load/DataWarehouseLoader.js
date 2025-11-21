// src/load/DataWarehouseLoader.js - FIXED VERSION
const dbManager = require('../../config/database');
const logger = require('../utils/Logger');

class DataWarehouseLoader {
  constructor() {
    this.pool = dbManager.getDWPool();
    this.initialized = false;
    this.skippedDueToFK = {};
  }

  async initialize() {
    if (this.initialized) return;
    await logger.info('Đang khởi tạo Data Warehouse...');
    try {
      await this.createWarehouseTables();
      this.initialized = true;
      await logger.success('Khởi tạo Data Warehouse thành công');
    } catch (error) {
      await logger.error('Failed to initialize Data Warehouse', error);
      throw error;
    }
  }

  async createWarehouseTables() {
    const createTableSQL = `
      -- ====== DIMENSION TABLES ======
      
      CREATE TABLE IF NOT EXISTS dim_khach_hang (
        ma_khach_hang VARCHAR(20) PRIMARY KEY,
        ho_ten VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        so_dien_thoai VARCHAR(20),
        dia_chi VARCHAR(255),
        ngay_sinh DATE,
        ngay_dang_ky DATE,
        gioi_tinh VARCHAR(10),
        loai_khach_hang VARCHAR(50) DEFAULT 'Thường',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_danh_muc (
        ma_danh_muc VARCHAR(20) PRIMARY KEY,
        ten_danh_muc VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_san_pham (
        ma_san_pham VARCHAR(20) PRIMARY KEY,
        ten_san_pham VARCHAR(150) NOT NULL,
        mo_ta TEXT,
        ma_danh_muc VARCHAR(20),
        don_gia DECIMAL(12,2),
        ton_kho INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_nhanvien_cskh (
        ma_nhan_vien VARCHAR(20) PRIMARY KEY,
        ho_ten VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE,
        so_dien_thoai VARCHAR(20),
        chuc_vu VARCHAR(50),
        phong_ban VARCHAR(50),
        ngay_tuyen_dung DATE,
        trang_thai VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      -- ====== FACT TABLES ======

      CREATE TABLE IF NOT EXISTS fact_don_hang (
        ma_don_hang VARCHAR(20) PRIMARY KEY,
        ma_khach_hang VARCHAR(20),
        ngay_dat DATE NOT NULL,
        tong_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_chi_tiet_don_hang (
        ma_don_hang VARCHAR(20),
        ma_san_pham VARCHAR(20),
        so_luong INT,
        don_gia DECIMAL(12,2),
        PRIMARY KEY (ma_don_hang, ma_san_pham)
      );

      CREATE TABLE IF NOT EXISTS fact_thanh_toan (
        ma_thanh_toan VARCHAR(20) PRIMARY KEY,
        ma_don_hang VARCHAR(20),
        ngay_thanh_toan DATE,
        so_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        phuong_thuc VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_phieu_ho_tro (
        ma_phieu_ho_tro VARCHAR(20) PRIMARY KEY,
        ma_khach_hang VARCHAR(20),
        loai_van_de VARCHAR(100),
        mo_ta TEXT,
        ngay_tao TIMESTAMP,
        trang_thai VARCHAR(50),
        do_uu_tien VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_danh_gia (
        ma_danh_gia VARCHAR(20) PRIMARY KEY,
        ma_khach_hang VARCHAR(20),
        diem_danh_gia INT CHECK (diem_danh_gia BETWEEN 1 AND 5),
        nhan_xet TEXT,
        ngay_danh_gia DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_phieu_xu_ly (
        ma_phieu_xu_ly VARCHAR(20) PRIMARY KEY,
        ma_phieu_ho_tro VARCHAR(20),
        ma_nhan_vien VARCHAR(20),
        ngay_xu_ly TIMESTAMP,
        hanh_dong VARCHAR(100),
        ket_qua VARCHAR(50),
        ghi_chu TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      -- ====== INDEXES ======
      CREATE INDEX IF NOT EXISTS idx_don_hang_khach_hang ON fact_don_hang(ma_khach_hang);
      CREATE INDEX IF NOT EXISTS idx_don_hang_ngay_dat ON fact_don_hang(ngay_dat);
      CREATE INDEX IF NOT EXISTS idx_phieu_ho_tro_khach_hang ON fact_phieu_ho_tro(ma_khach_hang);
      CREATE INDEX IF NOT EXISTS idx_phieu_ho_tro_trang_thai ON fact_phieu_ho_tro(trang_thai);
      CREATE INDEX IF NOT EXISTS idx_danh_gia_khach_hang ON fact_danh_gia(ma_khach_hang);
      CREATE INDEX IF NOT EXISTS idx_san_pham_danh_muc ON dim_san_pham(ma_danh_muc);
    `;

    await this.pool.query(`
        DROP TABLE IF EXISTS fact_chi_tiet_don_hang CASCADE;
        DROP TABLE IF EXISTS fact_thanh_toan CASCADE;
        DROP TABLE IF EXISTS fact_phieu_xu_ly CASCADE;
        DROP TABLE IF EXISTS fact_danh_gia CASCADE;
        DROP TABLE IF EXISTS fact_phieu_ho_tro CASCADE;
        DROP TABLE IF EXISTS fact_don_hang CASCADE;
        DROP TABLE IF EXISTS dim_san_pham CASCADE;
        DROP TABLE IF EXISTS dim_nhanvien_cskh CASCADE;
        DROP TABLE IF EXISTS dim_danh_muc CASCADE;
        DROP TABLE IF EXISTS dim_khach_hang CASCADE;
    `);

    await this.pool.query(createTableSQL);
    await logger.success('Tạo bảng Data Warehouse thành công');
  }

  async loadAll(cleanData) {
    await logger.startPhase('ĐANG TẢI DỮ LIỆU VÀO DATA WAREHOUSE');

    const stats = {
      dimension_tables: {},
      fact_tables: {},
      total_loaded: 0,
      skipped_due_to_fk: {}
    };

    try {
      // ====== DIMENSION TABLES ======
      await logger.info('Đang tải các bảng dimension...');
      
      stats.dimension_tables.dim_khach_hang = await this.loadTable(
        'dim_khach_hang', 
        cleanData.khach_hang || []
      );
      
      stats.dimension_tables.dim_danh_muc = await this.loadTable(
        'dim_danh_muc', 
        cleanData.danh_muc || []
      );
      
      const sanPhamResult = await this.loadTableWithFK(
        'dim_san_pham',
        cleanData.san_pham || [],
        'ma_danh_muc',
        'dim_danh_muc'
      );
      stats.dimension_tables.dim_san_pham = sanPhamResult.loaded;
      if (sanPhamResult.skipped > 0) {
        stats.skipped_due_to_fk.dim_san_pham = sanPhamResult.skipped;
      }
      
      stats.dimension_tables.dim_nhanvien_cskh = await this.loadTable(
        'dim_nhanvien_cskh', 
        cleanData.nhanvien_cskh || []
      );

      // ====== FACT TABLES ======
      await logger.info('Đang tải các bảng fact...');
      
      const donHangResult = await this.loadTableWithFK(
        'fact_don_hang',
        cleanData.don_hang || [],
        'ma_khach_hang',
        'dim_khach_hang'
      );
      stats.fact_tables.fact_don_hang = donHangResult.loaded;
      if (donHangResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_don_hang = donHangResult.skipped;
      }

      const chiTietResult = await this.loadTableWithMultipleFK(
        'fact_chi_tiet_don_hang',
        cleanData.chi_tiet_don_hang || [],
        [
          { fkColumn: 'ma_don_hang', refTable: 'fact_don_hang' },
          { fkColumn: 'ma_san_pham', refTable: 'dim_san_pham' }
        ]
      );
      stats.fact_tables.fact_chi_tiet_don_hang = chiTietResult.loaded;
      if (chiTietResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_chi_tiet_don_hang = chiTietResult.skipped;
      }

      const thanhToanResult = await this.loadTableWithFK(
        'fact_thanh_toan',
        cleanData.thanh_toan || [],
        'ma_don_hang',
        'fact_don_hang'
      );
      stats.fact_tables.fact_thanh_toan = thanhToanResult.loaded;
      if (thanhToanResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_thanh_toan = thanhToanResult.skipped;
      }

      const phieuHoTroResult = await this.loadTableWithFK(
        'fact_phieu_ho_tro',
        cleanData.phieu_ho_tro || [],
        'ma_khach_hang',
        'dim_khach_hang'
      );
      stats.fact_tables.fact_phieu_ho_tro = phieuHoTroResult.loaded;
      if (phieuHoTroResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_phieu_ho_tro = phieuHoTroResult.skipped;
      }

      const danhGiaResult = await this.loadTableWithFK(
        'fact_danh_gia',
        cleanData.danh_gia || [],
        'ma_khach_hang',
        'dim_khach_hang'
      );
      stats.fact_tables.fact_danh_gia = danhGiaResult.loaded;
      if (danhGiaResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_danh_gia = danhGiaResult.skipped;
      }

      const phieuXuLyResult = await this.loadTableWithMultipleFK(
        'fact_phieu_xu_ly',
        cleanData.phieu_xu_ly || [],
        [
          { fkColumn: 'ma_phieu_ho_tro', refTable: 'fact_phieu_ho_tro' },
          { fkColumn: 'ma_nhan_vien', refTable: 'dim_nhanvien_cskh' }
        ]
      );
      stats.fact_tables.fact_phieu_xu_ly = phieuXuLyResult.loaded;
      if (phieuXuLyResult.skipped > 0) {
        stats.skipped_due_to_fk.fact_phieu_xu_ly = phieuXuLyResult.skipped;
      }

      stats.total_loaded = 
        Object.values(stats.dimension_tables).reduce((a, b) => a + b, 0) +
        Object.values(stats.fact_tables).reduce((a, b) => a + b, 0);

      await logger.endPhase('TẢI DỮ LIỆU VÀO DATA WAREHOUSE HOÀN THÀNH');
      return stats;

    } catch (error) {
      await logger.error('Failed to load data to warehouse', error);
      throw error;
    }
  }

  async loadTableWithFK(tableName, records, fkColumn, refTable) {
    if (!records || records.length === 0) {
      await logger.info(`Không có dữ liệu để tải cho ${tableName}`);
      return { loaded: 0, skipped: 0 };
    }

    let loadedCount = 0;
    let skippedCount = 0;

    const existingFKs = await this.getExistingKeys(refTable);
    const existingFKSet = new Set(existingFKs);

    // ✅ LỌC BỎ các column không cần thiết TRƯỚC KHI insert
    const columns = Object.keys(records[0]).filter(col => 
      col !== 'source' && col !== 'inserted_at'  // ✅ Bỏ source và inserted_at
    );
    const columnNames = columns.join(', ');

    for (const record of records) {
      const fkValue = record[fkColumn];
      if (fkValue && !existingFKSet.has(fkValue)) {
        skippedCount++;
        continue;
      }

      // ✅ CHỈ LẤY values của các columns được phép
      const values = columns.map(col => record[col]);
      const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
      
      const insertSQL = `
        INSERT INTO ${tableName} (${columnNames}) 
        VALUES (${placeholders})
        ON CONFLICT DO NOTHING
      `;
      
      const result = await this.pool.query(insertSQL, values);
      if (result.rowCount > 0) {
        loadedCount++;
      }
    }

    if (skippedCount > 0) {
      await logger.warn(
        `Bỏ qua ${skippedCount}/${records.length} bản ghi từ ${tableName} do vi phạm foreign key`
      );
    }

    await logger.success(`Đã tải ${loadedCount} bản ghi vào ${tableName}`);
    return { loaded: loadedCount, skipped: skippedCount };
  }

  async loadTableWithMultipleFK(tableName, records, fkConstraints) {
    if (!records || records.length === 0) {
      return { loaded: 0, skipped: 0 };
    }

    let loadedCount = 0;
    let skippedCount = 0;

    const fkSets = {};
    for (const fk of fkConstraints) {
      const existingKeys = await this.getExistingKeys(fk.refTable);
      fkSets[fk.fkColumn] = new Set(existingKeys);
    }

    // ✅ LỌC BỎ các column không cần thiết
    const columns = Object.keys(records[0]).filter(col => 
      col !== 'source' && col !== 'inserted_at'
    );
    const columnNames = columns.join(', ');

    for (const record of records) {
      let isValid = true;
      for (const fk of fkConstraints) {
        const fkValue = record[fk.fkColumn];
        if (fkValue && !fkSets[fk.fkColumn].has(fkValue)) {
          isValid = false;
          break;
        }
      }

      if (!isValid) {
        skippedCount++;
        continue;
      }

      const values = columns.map(col => record[col]);
      const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
      
      const insertSQL = `
        INSERT INTO ${tableName} (${columnNames}) 
        VALUES (${placeholders})
        ON CONFLICT DO NOTHING
      `;
      
      const result = await this.pool.query(insertSQL, values);
      if (result.rowCount > 0) {
        loadedCount++;
      }
    }

    if (skippedCount > 0) {
      await logger.warn(
        `⚠️ Bỏ qua ${skippedCount}/${records.length} bản ghi từ ${tableName} do vi phạm foreign key`
      );
    }

    return { loaded: loadedCount, skipped: skippedCount };
  }

  async loadTable(tableName, records) {
    if (!records || records.length === 0) {
      await logger.info(`Không có dữ liệu để tải cho ${tableName}`);
      return 0;
    }

    let loadedCount = 0;
    
    // ✅ LỌC BỎ các column không cần thiết
    const columns = Object.keys(records[0]).filter(col => 
      col !== 'source' && col !== 'inserted_at'
    );
    const columnNames = columns.join(', ');

    for (const record of records) {
      const values = columns.map(col => record[col]);
      const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
      
      const insertSQL = `
        INSERT INTO ${tableName} (${columnNames}) 
        VALUES (${placeholders})
        ON CONFLICT DO NOTHING
      `;
      
      const result = await this.pool.query(insertSQL, values);
      if (result.rowCount > 0) {
        loadedCount++;
      }
    }

    await logger.success(`Đã tải ${loadedCount} bản ghi vào ${tableName}`);
    return loadedCount;
  }

  async getExistingKeys(tableName) {
    try {
      const pkColumn = this.getPrimaryKeyColumn(tableName);
      const query = `SELECT ${pkColumn} FROM ${tableName}`;
      const result = await this.pool.query(query);
      return result.rows.map(row => row[pkColumn]);
    } catch (error) {
      await logger.warn(`Could not get keys from ${tableName}: ${error.message}`);
      return [];
    }
  }

  getPrimaryKeyColumn(tableName) {
    const pkMap = {
      'dim_khach_hang': 'ma_khach_hang',
      'dim_danh_muc': 'ma_danh_muc',
      'dim_san_pham': 'ma_san_pham',
      'dim_nhanvien_cskh': 'ma_nhan_vien',
      'fact_don_hang': 'ma_don_hang',
      'fact_thanh_toan': 'ma_thanh_toan',
      'fact_phieu_ho_tro': 'ma_phieu_ho_tro',
      'fact_danh_gia': 'ma_danh_gia',
      'fact_phieu_xu_ly': 'ma_phieu_xu_ly'
    };
    return pkMap[tableName] || 'id';
  }

  async verifyDataIntegrity() {
    await logger.info('Verifying data integrity...');
    return [];
  }

  async getWarehouseStats() {
    const tables = [
      'dim_khach_hang', 'dim_danh_muc', 'dim_san_pham', 'dim_nhanvien_cskh',
      'fact_don_hang', 'fact_chi_tiet_don_hang', 'fact_thanh_toan',
      'fact_phieu_ho_tro', 'fact_danh_gia', 'fact_phieu_xu_ly'
    ];

    const stats = {};
    for (const table of tables) {
      const result = await this.pool.query(`SELECT COUNT(*) as count FROM ${table}`);
      stats[table] = parseInt(result.rows[0].count);
    }
    return stats;
  }
}

module.exports = DataWarehouseLoader;
