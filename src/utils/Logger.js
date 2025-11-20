// src/utils/Logger.js - System cho log.txt và console
const fs = require('fs-extra');
const path = require('path');
const moment = require('moment');

class Logger {
  constructor() {
    this.logDir = path.join(__dirname, '../../logs');
    this.logFile = path.join(this.logDir, 'log.log');
    this.errorLogFile = path.join(this.logDir, 'errorLog.log');
    this.initialized = false;
  }

  async initLogFiles() {
    if (this.initialized) return;
    
    await fs.ensureDir(this.logDir);
    
    // ✅ XÓA NỘI DUNG CŨ - Tạo file mới hoàn toàn
    await fs.writeFile(this.logFile, '');
    await fs.writeFile(this.errorLogFile, '');
    
    // Ghi header với timestamp
    const header = `${'='.repeat(80)}\n` +
                  `  ETL PIPELINE LOG - ${moment().format('YYYY-MM-DD HH:mm:ss')}\n` +
                  `${'='.repeat(80)}\n\n`;
    
    await fs.writeFile(this.logFile, header);
    await fs.writeFile(this.errorLogFile, header);
    
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

  // ✅ Log lỗi (console + errorLog.txt + log.txt) - GHI TẤT CẢ RECORDS
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
    
    // Ghi vào cả 2 file
    await fs.appendFile(this.errorLogFile, logMessage + '\n\n');
    await fs.appendFile(this.logFile, logMessage + '\n\n');
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
    console.log(`🛠 ${message}`);
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

  // ✅ KHÔNG CẦN clearLogs() nữa vì đã tự động xóa khi khởi tạo
    // Clear logs (cho testing)
  // async clearLogs() {
  //   await fs.writeFile(this.logFile, '');
  //   await fs.writeFile(this.errorLogFile, '');
  //   console.log('🗑️  Logs cleared');
  // }
}

module.exports = new Logger();