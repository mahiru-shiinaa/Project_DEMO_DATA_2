// src/utils/Logger.js - UPDATED
const fs = require('fs-extra');
const path = require('path');
const moment = require('moment');

class Logger {
  constructor() {
    this.logDir = path.join(__dirname, '../../logs');
    this.logFile = path.join(this.logDir, 'log.log');
    this.errorLogFile = path.join(this.logDir, 'errorLog.log');
    this.transformLogFile = path.join(this.logDir, 'transformLog.log'); // ✅ THÊM FILE MỚI
    this.initialized = false;
  }

  async initLogFiles() {
    if (this.initialized) return;
    
    await fs.ensureDir(this.logDir);
    
    // ✅ Xóa nội dung cũ - Tạo 3 file mới
    await fs.writeFile(this.logFile, '');
    await fs.writeFile(this.errorLogFile, '');
    await fs.writeFile(this.transformLogFile, ''); // ✅ THÊM
    
    // Ghi header với timestamp
    const header = `${'='.repeat(80)}\n` +
                  `  ETL PIPELINE LOG - ${moment().format('YYYY-MM-DD HH:mm:ss')}\n` +
                  `${'='.repeat(80)}\n\n`;
    
    await fs.writeFile(this.logFile, header);
    await fs.writeFile(this.errorLogFile, header);
    await fs.writeFile(this.transformLogFile, header); // ✅ THÊM
    
    this.initialized = true;
  }

  getTimestamp() {
    return moment().format('YYYY-MM-DD HH:mm:ss');
  }

  formatMessage(level, message, data = null) {
    let logMessage = `[${this.getTimestamp()}] [${level}] ${message}`;
    if (data) {
      logMessage += `\n${JSON.stringify(data, null, 2)}`;
    }
    return logMessage;
  }

  // Log thông tin chung (console + log.txt)
  async info(message, data = null) {
    await this.initLogFiles();
    const logMessage = this.formatMessage('INFO', message, data);
    console.log(`ℹ️  ${message}`);
    if (data) console.log(data);
    
    await fs.appendFile(this.logFile, logMessage + '\n\n');
  }

  // Log cảnh báo
  async warn(message, data = null) {
    await this.initLogFiles();
    const logMessage = this.formatMessage('WARN', message, data);
    console.warn(`⚠️  ${message}`);
    if (data) console.warn(data);
    
    await fs.appendFile(this.logFile, logMessage + '\n\n');
  }

  // Log lỗi CƠ BẢN
  async error(message, error = null, data = null) {
    await this.initLogFiles();
    
    const errorDetails = error ? {
      message: error.message,
      stack: error.stack,
      ...data
    } : data;

    const logMessage = this.formatMessage('ERROR', message, errorDetails);
    console.error(`❌ ${message}`);
    if (error) console.error(error);
    
    await fs.appendFile(this.errorLogFile, logMessage + '\n\n');
    await fs.appendFile(this.logFile, logMessage + '\n\n');
  }

  // ✅ Log lỗi với chi tiết RECORDS (tách biệt)
  async errorRecords(tableName, errorRecords) {
    await this.initLogFiles();
    
    const count = errorRecords.length;
    
    // 1. GHI THÔNG BÁO TÓM TẮT VÀO log.log
    const summaryMessage = `[${this.getTimestamp()}] [ERROR] Đã ghi lại ${count} records lỗi của table: ${tableName} vào errorLog.log\n`;
    console.error(`❌ Đã ghi lại ${count} records lỗi của table: ${tableName}`);
    await fs.appendFile(this.logFile, summaryMessage + '\n');
    
    // 2. GHI CHI TIẾT ĐẦY ĐỦ VÀO errorLog.log
    const detailMessage = this.formatMessage('ERROR', `Đã ghi lại records lỗi của table: ${tableName}`, {
      count: count,
      allErrors: errorRecords
    });
    await fs.appendFile(this.errorLogFile, detailMessage + '\n\n');
  }

  // ✅ METHOD MỚI: Log transform details (tách biệt log.log và transformLog.log)
  async transformRecords(tableName, transformLogs) {
    await this.initLogFiles();
    
    const count = transformLogs.length;
    
    // ✅ 1. GHI THÔNG BÁO TÓM TẮT VÀO log.log
    const summaryMessage = `[${this.getTimestamp()}] [INFO] Đã transform ${count} records của table: ${tableName} - Chi tiết xem tại transformLog.log\n`;
    console.log(`🔄 Đã transform ${count} records của table: ${tableName}`);
    await fs.appendFile(this.logFile, summaryMessage + '\n');
    
    // ✅ 2. GHI CHI TIẾT ĐẦY ĐỦ VÀO transformLog.log
    const detailMessage = this.formatMessage('TRANSFORM', `Chi tiết transform của table: ${tableName}`, {
      table: tableName,
      totalTransformed: count,
      allTransforms: transformLogs
    });
    await fs.appendFile(this.transformLogFile, detailMessage + '\n\n');
  }

  // Log thành công
  async success(message, data = null) {
    await this.initLogFiles();
    const logMessage = this.formatMessage('SUCCESS', message, data);
    console.log(`✅ ${message}`);
    if (data) console.log(data);
    
    await fs.appendFile(this.logFile, logMessage + '\n\n');
  }

  // Log debug
  async debug(message, data = null) {
    await this.initLogFiles();
    const logMessage = this.formatMessage('DEBUG', message, data);
    console.log(`🛠  ${message}`);
    if (data) console.log(data);
    
    await fs.appendFile(this.logFile, logMessage + '\n\n');
  }

  // Log bắt đầu một phase
  async startPhase(phaseName) {
    await this.initLogFiles();
    const separator = '='.repeat(80);
    const message = `\n${separator}\n  ${phaseName} STARTED\n${separator}\n`;
    console.log(message);
    await fs.appendFile(this.logFile, message);
  }

  // Log kết thúc một phase
  async endPhase(phaseName, stats = null) {
    await this.initLogFiles();
    const separator = '='.repeat(80);
    let message = `\n${separator}\n  ${phaseName} COMPLETED`;
    if (stats) {
      message += `\n  Stats: ${JSON.stringify(stats)}`;
    }
    message += `\n${separator}\n\n`;
    
    console.log(message);
    await fs.appendFile(this.logFile, message);
  }

  // Log thống kê
  async stats(title, statistics) {
    await this.initLogFiles();
    const message = this.formatMessage('STATS', title, statistics);
    console.log(`📊 ${title}:`, statistics);
    await fs.appendFile(this.logFile, message + '\n\n');
  }
}

module.exports = new Logger();