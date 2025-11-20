// src/staging/StagingService.js - Quản lý Staging Database
const dbManager = require("../../config/database");
const logger = require("../utils/Logger");

class StagingService {
  constructor() {
    this.pool = dbManager.getStagingPool();
    this.initialized = false;
  }

  /**
   * Khởi tạo staging tables
   */
  async initialize() {
    if (this.initialized) return;

    await logger.info("Initializing staging database...");

    try {
      // Tạo schema cho staging nếu chưa có
      await this.createStagingTables();

      // Clear staging data cũ
      await this.clearAllTables();

      this.initialized = true;
      await logger.success("Staging database initialized");
    } catch (error) {
      await logger.error("Failed to initialize staging database", error);
      throw error;
    }
  }

  /**
   * Tạo staging tables
   */
  async createStagingTables() {
    // NEW: Drop old tables trước khi tạo mới (dev mode)
  await logger.info("Dropping old staging tables...");

  const dropTables = [
    "stg_phieu_xu_ly",
    "stg_nhanvien_cskh",
    "stg_danh_gia",
    "stg_phieu_ho_tro",
    "stg_thanh_toan",
    "stg_chi_tiet_don_hang",
    "stg_don_hang",
    "stg_san_pham",
    "stg_danh_muc",
    "stg_khach_hang"
  ];

  for (const table of dropTables) {
    await this.pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
  }

  await logger.success("Old staging tables dropped");

    // Tạo staging tables
    const tables = [
      // Tables từ Data Source 1
      `CREATE TABLE IF NOT EXISTS stg_khach_hang (
  ma_khach_hang VARCHAR(10),
  ho_ten VARCHAR(100),
  email VARCHAR(100),
  so_dien_thoai VARCHAR(20),
  dia_chi VARCHAR(255),
  ngay_sinh DATE,
  ngay_dang_ky DATE,
  gioi_tinh VARCHAR(10),
  loai_khach_hang VARCHAR(50), -- <<--- THÊM DÒNG NÀY
  source VARCHAR(20),
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,

      `CREATE TABLE IF NOT EXISTS stg_danh_muc (
        ma_danh_muc VARCHAR(10),
        ten_danh_muc VARCHAR(100),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_san_pham (
        ma_san_pham VARCHAR(10),
        ten_san_pham VARCHAR(150),
        mo_ta TEXT,
        ma_danh_muc VARCHAR(10),
        don_gia DECIMAL(12,2),
        ton_kho INT,
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_don_hang (
        ma_don_hang VARCHAR(10),
        ma_khach_hang VARCHAR(10),
        ngay_dat DATE,
        tong_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_chi_tiet_don_hang (
        ma_don_hang VARCHAR(10),
        ma_san_pham VARCHAR(10),
        so_luong INT,
        don_gia DECIMAL(12,2),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_thanh_toan (
        ma_thanh_toan VARCHAR(10),
        ma_don_hang VARCHAR(10),
        ngay_thanh_toan DATE,
        so_tien DECIMAL(12,2),
        trang_thai VARCHAR(50),
        phuong_thuc VARCHAR(50),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      // Tables từ Data Source 2
      `CREATE TABLE IF NOT EXISTS stg_phieu_ho_tro (
        ma_phieu_ho_tro VARCHAR(10),
        ma_khach_hang VARCHAR(10),
        loai_van_de VARCHAR(100),
        mo_ta TEXT,
        ngay_tao TIMESTAMP,
        trang_thai VARCHAR(50),
        do_uu_tien VARCHAR(20),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_danh_gia (
        ma_danh_gia VARCHAR(10),
        ma_khach_hang VARCHAR(10),
        diem_danh_gia INT,
        nhan_xet TEXT,
        ngay_danh_gia DATE,
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_nhanvien_cskh (
        ma_nhan_vien VARCHAR(10),
        ho_ten VARCHAR(100),
        email VARCHAR(100),
        so_dien_thoai VARCHAR(20),
        chuc_vu VARCHAR(50),
        phong_ban VARCHAR(50),
        ngay_tuyen_dung DATE,
        trang_thai VARCHAR(20),
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,

      `CREATE TABLE IF NOT EXISTS stg_phieu_xu_ly (
        ma_phieu_xu_ly VARCHAR(10),
        ma_phieu_ho_tro VARCHAR(10),
        ma_nhan_vien VARCHAR(10),
        ngay_xu_ly TIMESTAMP,
        hanh_dong VARCHAR(100),
        ket_qua VARCHAR(50),
        ghi_chu TEXT,
        source VARCHAR(20),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
    ];

    for (const createTableSQL of tables) {
      await this.pool.query(createTableSQL);
    }

    await logger.success("All staging tables created");
  }

  /**
   * Insert records vào staging table
   */
  async insertRecords(tableName, records, source) {
    if (!records || records.length === 0) {
      return 0;
    }

    const stagingTableName = `stg_${tableName}`;

    try {
      // Lấy columns từ record đầu tiên
      const columns = Object.keys(records[0]);
      const columnNames = [...columns, "source"].join(", ");

      let insertedCount = 0;

      // Insert từng record
      for (const record of records) {
        const values = columns.map((col) => record[col]);
        values.push(source);

        const placeholders = values.map((_, i) => `$${i + 1}`).join(", ");
        const insertSQL = `INSERT INTO ${stagingTableName} (${columnNames}) VALUES (${placeholders})`;

        await this.pool.query(insertSQL, values);
        insertedCount++;
      }

      await logger.success(
        `Inserted ${insertedCount} records into ${stagingTableName}`
      );
      return insertedCount;
    } catch (error) {
      await logger.error(`Failed to insert into ${stagingTableName}`, error);
      throw error;
    }
  }

  /**
   * Lấy tất cả dữ liệu từ staging
   */
  async getAllData() {
    const tables = [
      "khach_hang",
      "danh_muc",
      "san_pham",
      "don_hang",
      "chi_tiet_don_hang",
      "thanh_toan",
      "phieu_ho_tro",
      "danh_gia",
      "nhanvien_cskh",
      "phieu_xu_ly",
    ];

    const data = {};

    for (const table of tables) {
      const stagingTableName = `stg_${table}`;
      try {
        const result = await this.pool.query(
          `SELECT * FROM ${stagingTableName}`
        );
        data[table] = result.rows;
      } catch (error) {
        await logger.warn(`Table ${stagingTableName} might not exist or empty`);
        data[table] = [];
      }
    }

    return data;
  }

  /**
   * Clear tất cả staging tables
   */
  async clearAllTables() {
    const tables = [
      "stg_khach_hang",
      "stg_danh_muc",
      "stg_san_pham",
      "stg_don_hang",
      "stg_chi_tiet_don_hang",
      "stg_thanh_toan",
      "stg_phieu_ho_tro",
      "stg_danh_gia",
      "stg_nhanvien_cskh",
      "stg_phieu_xu_ly",
    ];

    for (const table of tables) {
      try {
        await this.pool.query(`TRUNCATE TABLE ${table} CASCADE`);
      } catch (error) {
        // Table might not exist yet
      }
    }

    await logger.info("Staging tables cleared");
  }

  /**
   * Đếm records trong staging table
   */
  async countRecords(tableName) {
    const stagingTableName = `stg_${tableName}`;
    try {
      const result = await this.pool.query(
        `SELECT COUNT(*) as count FROM ${stagingTableName}`
      );
      return parseInt(result.rows[0].count);
    } catch (error) {
      return 0;
    }
  }

  /**
   * Lấy thống kê staging
   */
  async getStats() {
    const tables = [
      "khach_hang",
      "danh_muc",
      "san_pham",
      "don_hang",
      "chi_tiet_don_hang",
      "thanh_toan",
      "phieu_ho_tro",
      "danh_gia",
      "nhanvien_cskh",
      "phieu_xu_ly",
    ];

    const stats = {};
    let total = 0;

    for (const table of tables) {
      const count = await this.countRecords(table);
      stats[table] = count;
      total += count;
    }

    stats.total = total;
    return stats;
  }
}

module.exports = StagingService;
