// src/extract/CsvExtractor.js - Đọc dữ liệu từ CSV files
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const logger = require('../utils/Logger');

class CsvExtractor {
  constructor() {
    this.csvPath = process.env.CSV_PATH || './data/datasource2';
    this.csvFiles = [
      'khach_hang.csv',
      'phieu_ho_tro.csv',
      'danh_gia.csv',
      'nhanvien_cskh.csv',
      'phieu_xu_ly.csv'
    ];
  }

  // Extract tất cả CSV files
  async extractAll() {
    await logger.startPhase('EXTRACT - CSV Data Source 2');
    
    const results = {};
    let totalRecords = 0;

    try {
      for (const fileName of this.csvFiles) {
        const tableName = path.basename(fileName, '.csv');
        await logger.info(`Extracting CSV: ${fileName}`);
        
        const data = await this.extractCsv(fileName);
        results[tableName] = data;
        totalRecords += data.length;

        await logger.success(`Extracted ${data.length} records from ${fileName}`);
      }

      await logger.endPhase('EXTRACT - CSV Data Source 2', {
        totalFiles: this.csvFiles.length,
        totalRecords: totalRecords
      });

      return results;
    } catch (error) {
      await logger.error('Failed to extract from CSV files', error);
      throw error;
    }
  }

  // Extract một file CSV cụ thể
  async extractCsv(fileName) {
    return new Promise((resolve, reject) => {
      const results = [];
      const filePath = path.join(this.csvPath, fileName);

      // Kiểm tra file tồn tại
      if (!fs.existsSync(filePath)) {
        const error = new Error(`CSV file not found: ${filePath}`);
        logger.error(`File not found: ${fileName}`, error);
        reject(error);
        return;
      }

      fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          trim: true,
          // Xử lý headers với whitespace
          mapHeaders: ({ header }) => header.trim()
        }))
        .on('data', (row) => {
          // Trim tất cả các giá trị
          const cleanedRow = {};
          for (const key in row) {
            cleanedRow[key] = typeof row[key] === 'string' ? row[key].trim() : row[key];
          }
          results.push(cleanedRow);
        })
        .on('end', () => {
          resolve(results);
        })
        .on('error', (error) => {
          logger.error(`Error reading CSV: ${fileName}`, error);
          reject(error);
        });
    });
  }

  // Extract CSV với filter
  async extractWithFilter(fileName, filterFn) {
    const allData = await this.extractCsv(fileName);
    const filteredData = allData.filter(filterFn);
    
    await logger.info(`Filtered ${filteredData.length}/${allData.length} records from ${fileName}`);
    return filteredData;
  }

  // Extract và convert types
  async extractWithTypeConversion(fileName, typeMap) {
    const data = await this.extractCsv(fileName);
    
    return data.map(row => {
      const converted = { ...row };
      for (const [field, type] of Object.entries(typeMap)) {
        if (converted[field] !== undefined && converted[field] !== null && converted[field] !== '') {
          switch (type) {
            case 'number':
              converted[field] = parseFloat(converted[field]);
              break;
            case 'integer':
              converted[field] = parseInt(converted[field]);
              break;
            case 'boolean':
              converted[field] = converted[field].toLowerCase() === 'true';
              break;
            case 'date':
              converted[field] = new Date(converted[field]);
              break;
            // Mặc định giữ nguyên string
          }
        }
      }
      return converted;
    });
  }

  // Kiểm tra tất cả CSV files có tồn tại không
  async validateFiles() {
    const missingFiles = [];
    
    for (const fileName of this.csvFiles) {
      const filePath = path.join(this.csvPath, fileName);
      if (!fs.existsSync(filePath)) {
        missingFiles.push(fileName);
      }
    }

    if (missingFiles.length > 0) {
      await logger.error('Missing CSV files', null, { missingFiles });
      throw new Error(`Missing CSV files: ${missingFiles.join(', ')}`);
    }

    await logger.success('All CSV files validated');
    return true;
  }
}

module.exports = CsvExtractor;