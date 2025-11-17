// src/load/DataWarehouseLoader.js - Load clean data vào Data Warehouse
const dbManager = require('../../config/database');
const logger = require('../utils/Logger');

class DataWarehouseLoader {
  constructor() {
    this.pool = dbManager.getDWPool();
    this.initialized = false;
  }

  /**
   * Khởi tạo Data Warehouse schema
   */
  async initialize() {
    if (this.initialized) return;

    await logger.info('Initializing Data Warehouse...');

    try {
      await this.createWarehouseTables();
      this.initialized = true;
      await logger.success('Data Warehouse initialized');
    } catch (error) {
      await logger.error('Failed to initialize Data Warehouse', error);
      throw error;
    }
  }

  /**
   * Tạo các bảng trong Data Warehouse
   */
  async createWarehouseTables() {
    const createTableSQL = `
      -- Dimension Tables
      
      CREATE TABLE IF NOT EXISTS dim_khach_hang (
        ma_khach_hang VARCHAR(10) PRIMARY KEY,
        ho_ten VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        so_dien_thoai VARCHAR(20),
        dia_chi VARCHAR(255),
        ngay_sinh DATE,
        ngay_dang_ky DATE,
        gioi_tinh VARCHAR(10),
        loai_khach_hang VARCHAR(50),
        sources VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_danh_muc (
        ma_danh_muc VARCHAR(10) PRIMARY KEY,
        ten_danh_muc VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_san_pham (
        ma_san_pham VARCHAR(10) PRIMARY KEY,
        ten_san_pham VARCHAR(150) NOT NULL,
        mo_ta TEXT,
        ma_danh_muc VARCHAR(10) REFERENCES dim_danh_muc(ma_danh_muc),
        don_gia DECIMAL(12,2),
        ton_kho INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dim_nhanvien_cskh (
        ma_nhan_vien VARCHAR(10) PRIMARY KEY,
        ho_ten VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE,
        so_dien_thoai VARCHAR(20),
        chuc_vu VARCHAR(50),
        phong_ban VARCHAR(50),
        ngay_tuyen_dung DATE,
        trang_thai VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      -- Fact Tables

      CREATE TABLE IF NOT EXISTS fact_don_hang (
        ma_don_hang VARCHAR(10) PRIMARY KEY,
        ma_khach_hang VARCHAR(10) REFERENCES dim_khach_hang(ma_khach_hang),
        ngay_dat DATE NOT NULL,
        tong_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_chi_tiet_don_hang (
        ma_don_hang VARCHAR(10) REFERENCES fact_don_hang(ma_don_hang) ON DELETE CASCADE,
        ma_san_pham VARCHAR(10) REFERENCES dim_san_pham(ma_san_pham),
        so_luong INT,
        don_gia DECIMAL(12,2),
        PRIMARY KEY (ma_don_hang, ma_san_pham)
      );

      CREATE TABLE IF NOT EXISTS fact_thanh_toan (
        ma_thanh_toan VARCHAR(10) PRIMARY KEY,
        ma_don_hang VARCHAR(10) REFERENCES fact_don_hang(ma_don_hang) ON DELETE CASCADE,
        ngay_thanh_toan DATE,
        so_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        phuong_thuc VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_phieu_ho_tro (
        ma_phieu_ho_tro VARCHAR(10) PRIMARY KEY,
        ma_khach_hang VARCHAR(10) REFERENCES dim_khach_hang(ma_khach_hang),
        loai_van_de VARCHAR(100),
        mo_ta TEXT,
        ngay_tao TIMESTAMP,
        trang_thai VARCHAR(50),
        do_uu_tien VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_danh_gia (
        ma_danh_gia VARCHAR(10) PRIMARY KEY,
        ma_khach_hang VARCHAR(10) REFERENCES dim_khach_hang(ma_khach_hang),
        diem_danh_gia INT CHECK (diem_danh_gia BETWEEN 1 AND 5),
        nhan_xet TEXT,
        ngay_danh_gia DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS fact_phieu_xu_ly (
        ma_phieu_xu_ly VARCHAR(10) PRIMARY KEY,
        ma_phieu_ho_tro VARCHAR(10) REFERENCES fact_phieu_ho_tro(ma_phieu_ho_tro) ON DELETE CASCADE,
        ma_nhan_vien VARCHAR(10) REFERENCES dim_nhanvien_cskh(ma_nhan_vien),
        ngay_xu_ly TIMESTAMP,
        hanh_dong VARCHAR(100),
        ket_qua VARCHAR(50),
        ghi_chu TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      -- Indexes for performance
      CREATE INDEX IF NOT EXISTS idx_don_hang_khach_hang ON fact_don_hang(ma_khach_hang);
      CREATE INDEX IF NOT EXISTS idx_don_hang_ngay_dat ON fact_don_hang(ngay_dat);
      CREATE INDEX IF NOT EXISTS idx_phieu_ho_tro_khach_hang ON fact_phieu_ho_tro(ma_khach_hang);
      CREATE INDEX IF NOT EXISTS idx_phieu_ho_tro_trang_thai ON fact_phieu_ho_tro(trang_thai);
      CREATE INDEX IF NOT EXISTS idx_danh_gia_khach_hang ON fact_danh_gia(ma_khach_hang);
    `;

    await this.pool.query(createTableSQL);
    await logger.success('Data Warehouse tables created');
  }

  /**
   * Load tất cả clean data vào warehouse
   */
  async loadAll(cleanData) {
    await logger.startPhase('LOADING TO DATA WAREHOUSE');

    const stats = {
      dimension_tables: {},
      fact_tables: {},
      total_loaded: 0
    };

    try {
      // Load theo thứ tự: Dimensions trước, Facts sau
      
      // 1. Load Dimension Tables
      await logger.info('Loading dimension tables...');
      
      stats.dimension_tables.dim_khach_hang = await this.loadTable('dim_khach_hang', cleanData.khach_hang || []);
      stats.dimension_tables.dim_danh_muc = await this.loadTable('dim_danh_muc', cleanData.danh_muc || []);
      stats.dimension_tables.dim_san_pham = await this.loadTable('dim_san_pham', cleanData.san_pham || []);
      stats.dimension_tables.dim_nhanvien_cskh = await this.loadTable('dim_nhanvien_cskh', cleanData.nhanvien_cskh || []);

      // 2. Load Fact Tables
      await logger.info('Loading fact tables...');
      
      stats.fact_tables.fact_don_hang = await this.loadTable('fact_don_hang', cleanData.don_hang || []);
      stats.fact_tables.fact_chi_tiet_don_hang = await this.loadTable('fact_chi_tiet_don_hang', cleanData.chi_tiet_don_hang || []);
      stats.fact_tables.fact_thanh_toan = await this.loadTable('fact_thanh_toan', cleanData.thanh_toan || []);
      stats.fact_tables.fact_phieu_ho_tro = await this.loadTable('fact_phieu_ho_tro', cleanData.phieu_ho_tro || []);
      stats.fact_tables.fact_danh_gia = await this.loadTable('fact_danh_gia', cleanData.danh_gia || []);
      stats.fact_tables.fact_phieu_xu_ly = await this.loadTable('fact_phieu_xu_ly', cleanData.phieu_xu_ly || []);

      // Tính tổng
      stats.total_loaded = Object.values(stats.dimension_tables).reduce((a, b) => a + b, 0) +
                          Object.values(stats.fact_tables).reduce((a, b) => a + b, 0);

      await logger.endPhase('LOADING TO DATA WAREHOUSE', stats);
      return stats;

    } catch (error) {
      await logger.error('Failed to load data to warehouse', error);
      throw error;
    }
  }

  /**
   * Load data vào một table
   */
  async loadTable(tableName, records) {
    if (!records || records.length === 0) {
      await logger.info(`No data to load for ${tableName}`);
      return 0;
    }

    let loadedCount = 0;
    
    try {
      // Clear table trước (optional, tùy yêu cầu)
      // await this.pool.query(`TRUNCATE TABLE ${tableName} CASCADE`);

      // Lấy columns từ record đầu tiên (bỏ qua source, inserted_at)
      const columns = Object.keys(records[0]).filter(col => 
        col !== 'source' && col !== 'inserted_at' && col !== 'sources'
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

      await logger.success(`✓ Loaded ${loadedCount} records into ${tableName}`);
      return loadedCount;

    } catch (error) {
      await logger.error(`Failed to load ${tableName}`, error);
      throw error;
    }
  }

  /**
   * Verify data integrity
   */
  async verifyDataIntegrity() {
    await logger.info('Verifying data integrity...');

    const checks = [
      // Kiểm tra foreign keys
      {
        name: 'fact_don_hang references dim_khach_hang',
        query: `
          SELECT COUNT(*) as count 
          FROM fact_don_hang dh 
          LEFT JOIN dim_khach_hang kh ON dh.ma_khach_hang = kh.ma_khach_hang 
          WHERE kh.ma_khach_hang IS NULL
        `
      },
      {
        name: 'dim_san_pham references dim_danh_muc',
        query: `
          SELECT COUNT(*) as count 
          FROM dim_san_pham sp 
          LEFT JOIN dim_danh_muc dm ON sp.ma_danh_muc = dm.ma_danh_muc 
          WHERE sp.ma_danh_muc IS NOT NULL AND dm.ma_danh_muc IS NULL
        `
      }
    ];

    const results = [];
    for (const check of checks) {
      const result = await this.pool.query(check.query);
      const count = parseInt(result.rows[0].count);
      results.push({
        check: check.name,
        orphanedRecords: count,
        passed: count === 0
      });

      if (count > 0) {
        await logger.warn(`Integrity check failed: ${check.name} has ${count} orphaned records`);
      }
    }

    await logger.info('Data integrity verification completed', results);
    return results;
  }

  /**
   * Lấy thống kê warehouse
   */
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