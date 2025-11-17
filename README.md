# 🚀 Dự án Tích hợp Dữ liệu ETL

## 📌 Thông tin Dự án
**Đề tài:** TÍCH HỢP DỮ LIỆU GIỮA HỆ THỐNG THƯƠNG MẠI ĐIỆN TỬ VÀ TRUNG TÂM CHĂM SÓC KHÁCH HÀNG

**Công nghệ:** Node.js, RabbitMQ, PostgreSQL, Docker, Regex, Design Patterns

**Mô hình:** ETL (Extract, Transform, Load)

---

## 🏗️ Kiến trúc Hệ thống

```
┌─────────────────┐       ┌─────────────────┐
│  Data Source 1  │       │  Data Source 2  │
│   (PostgreSQL)  │       │   (CSV Files)   │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │      EXTRACT            │
         └────────┬────────────────┘
                  │
         ┌────────▼────────┐
         │   RabbitMQ      │
         │   (Queues)      │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │   Staging DB    │
         │  (PostgreSQL)   │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │ Data Quality    │
         │ - Deduplication │
         │ - Validation    │
         │ - Transform     │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │ Data Warehouse  │
         │  (PostgreSQL)   │
         │  Clean Data     │
         └─────────────────┘
```

---

## 📂 Cấu trúc Thư mục

```
etl-ecommerce-integration/
├── config/
│   ├── database.js         # Kết nối PostgreSQL
│   ├── rabbitmq.js         # Kết nối RabbitMQ
│   └── constants.js        # Enum values & constants
│
├── data/
│   └── datasource2/        # CSV files
│       ├── khach_hang.csv
│       ├── phieu_ho_tro.csv
│       ├── danh_gia.csv
│       ├── nhanvien_cskh.csv
│       └── phieu_xu_ly.csv
│
├── src/
│   ├── extract/
│   │   ├── PostgresExtractor.js
│   │   └── CsvExtractor.js
│   │
│   ├── queue/
│   │   ├── Producer.js
│   │   └── Consumer.js
│   │
│   ├── staging/
│   │   ├── StagingService.js
│   │   └── schema.sql
│   │
│   ├── validation/
│   │   ├── rules/
│   │   │   ├── HoTen.rule.js
│   │   │   ├── Email.rule.js
│   │   │   ├── SoDienThoai.rule.js
│   │   │   ├── NgaySinh.rule.js
│   │   │   └── ... (thêm rules khác)
│   │   ├── RuleEngine.js
│   │   └── ValidationService.js
│   │
│   ├── transform/
│   │   ├── transformers/
│   │   │   ├── HoTenTransformer.js
│   │   │   ├── EmailTransformer.js
│   │   │   └── ... (thêm transformers khác)
│   │   ├── TransformEngine.js
│   │   └── TransformService.js
│   │
│   ├── deduplication/
│   │   └── DeduplicationService.js
│   │
│   ├── load/
│   │   ├── DataWarehouseLoader.js
│   │   └── schema.sql
│   │
│   └── utils/
│       ├── Logger.js
│       └── helpers.js
│
├── logs/
│   ├── log.txt
│   └── errorLog.txt
│
├── docker-compose.yml
├── package.json
├── .env
└── index.js
```

---

## 🎯 Design Patterns được sử dụng

### 1. **Chain of Responsibility Pattern** (RuleEngine)
Mỗi rule là một handler trong chain, xử lý validation tuần tự.

```javascript
// Ví dụ: HoTen.rule.js
class HoTenRule {
  validate(value, record) {
    // Validation logic
    return { isValid, canFix, errors };
  }
}
```

### 2. **Strategy Pattern** (TransformEngine)
Mỗi transformer là một strategy khác nhau để transform data.

```javascript
// Ví dụ: HoTenTransformer.js
class HoTenTransformer {
  transform(value, record, errors) {
    // Transform logic
    return transformedValue;
  }
}
```

### 3. **Singleton Pattern** (Database, RabbitMQ Managers)
Đảm bảo chỉ có 1 instance cho connections.

---

## 🔧 Cài đặt và Chạy

### 1. **Cài đặt Dependencies**

```bash
npm install
```

### 2. **Cấu hình Environment Variables**

Tạo file `.env`:

```env
# PostgreSQL Data Source 1
DS1_HOST=ep-frosty-feather-a1fgkkhk-pooler.ap-southeast-1.aws.neon.tech
DS1_DATABASE=Data_Source_1
DS1_USER=neondb_owner
DS1_PASSWORD=npg_ev5qPxOBAn3E
DS1_PORT=5432
DS1_SSL=true

# Local databases
DS2_HOST=localhost
DS2_DATABASE=staging_db
DS2_USER=postgres
DS2_PASSWORD=postgres
DS2_PORT=5433

DW_HOST=localhost
DW_DATABASE=datawarehouse
DW_USER=postgres
DW_PASSWORD=postgres
DW_PORT=5434

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672

# Paths
CSV_PATH=./data/datasource2
LOG_PATH=./logs
```

### 3. **Khởi động Docker Services**

```bash
docker-compose up -d
```

Các services:
- RabbitMQ: http://localhost:15672 (guest/guest)
- Staging DB: localhost:5433
- Warehouse DB: localhost:5434
- pgAdmin: http://localhost:5050 (admin@admin.com/admin)

### 4. **Chuẩn bị CSV Data**

Đặt các file CSV vào `./data/datasource2/`:
- khach_hang.csv
- phieu_ho_tro.csv
- danh_gia.csv
- nhanvien_cskh.csv
- phieu_xu_ly.csv

### 5. **Chạy ETL Pipeline**

```bash
npm start
```

---

## 📝 Luồng hoạt động chi tiết

### **PHASE 1: EXTRACT**
1. Đọc dữ liệu từ PostgreSQL (Data Source 1) - 6 bảng
2. Đọc dữ liệu từ CSV files (Data Source 2) - 5 files
3. Log số lượng records đã extract

### **PHASE 2: QUEUE & STAGING**
1. Producer đẩy dữ liệu vào RabbitMQ queues:
   - Queue 1: datasource1_queue (PostgreSQL data)
   - Queue 2: datasource2_queue (CSV data)
2. Consumer nhận messages từ queues
3. Lưu tất cả vào Staging Database (raw data)

### **PHASE 3: DATA QUALITY**

#### 3.1 Deduplication
- Loại bỏ records trùng lặp dựa trên primary key
- Log số lượng records đã loại bỏ

#### 3.2 Validation (Chain of Responsibility)
- Áp dụng các rules cho từng field:
  - `HoTen.rule.js`: Không null, không chứa số, có ít nhất 2 từ,...
  - `Email.rule.js`: Đúng format, không chứa khoảng trắng,...
  - `SoDienThoai.rule.js`: Đúng format số VN
  - `NgaySinh.rule.js`: Tuổi hợp lệ (13-120)
  - `TrangThai.rule.js`: Giá trị thuộc enum
  - `DonGia.rule.js`: Không âm, trong khoảng hợp lệ
  - ... (thêm rules khác)
- Phân loại:
  - **Valid records**: Dữ liệu đúng
  - **Fixable errors**: Lỗi có thể sửa (viết tắt, lowercase,...)
  - **Unfixable errors**: Lỗi không thể sửa (null, format sai hoàn toàn)

#### 3.3 Transform (Strategy Pattern)
- Chỉ transform records có **fixable errors**:
  - `HoTenTransformer`: Chuẩn hóa viết hoa, mở rộng viết tắt
  - `EmailTransformer`: Chuyển lowercase, xóa khoảng trắng
  - `TrangThaiTransformer`: Sửa trạng thái sai
  - `NgayThangTransformer`: Sửa định dạng ngày
  - ... (thêm transformers khác)

#### 3.4 Re-validation
- Validate lại sau khi transform
- Tách thành:
  - **Clean data**: Dữ liệu sạch → Load vào warehouse
  - **Error data**: Vẫn còn lỗi → Ghi log

### **PHASE 4: LOAD**
1. Load clean data vào Data Warehouse
2. Tạo các bảng dimension và fact tables
3. Đảm bảo mối quan hệ (foreign keys)

---

## 📊 Bảng trong Data Warehouse

### **Dimension Tables**
1. `dim_khach_hang` - Khách hàng (merge từ cả 2 nguồn)
2. `dim_san_pham` - Sản phẩm
3. `dim_danh_muc` - Danh mục
4. `dim_nhanvien_cskh` - Nhân viên CSKH

### **Fact Tables**
5. `fact_don_hang` - Đơn hàng
6. `fact_chi_tiet_don_hang` - Chi tiết đơn hàng
7. `fact_thanh_toan` - Thanh toán
8. `fact_phieu_ho_tro` - Phiếu hỗ trợ
9. `fact_danh_gia` - Đánh giá
10. `fact_phieu_xu_ly` - Phiếu xử lý

---

## 🧪 Testing

### Test từng component:

```javascript
// Test Extract
const extractor = new PostgresExtractor();
const data = await extractor.extractAll();
console.log(data);

// Test Validation
const ruleEngine = new RuleEngine();
const result = ruleEngine.validateRecord(record);
console.log(result);

// Test Transform
const transformEngine = new TransformEngine();
const transformed = transformEngine.transformRecord(record, validationResult);
console.log(transformed);
```

---

## 📋 Phân công công việc nhóm

### **Thành viên 1** - RabbitMQ & Infrastructure
- Setup Docker, RabbitMQ
- Config database connections
- Producer, Consumer

### **Thành viên 2** - Validation Rules (Part 1)
- HoTen.rule.js
- Email.rule.js
- SoDienThoai.rule.js

### **Thành viên 3** - Validation Rules (Part 2)
- NgaySinh.rule.js
- TrangThai.rule.js
- DonGia.rule.js

### **Thành viên 4** - Transformers (Part 1)
- HoTenTransformer.js
- EmailTransformer.js
- SoDienThoaiTransformer.js

### **Thành viên 5** - Transformers (Part 2)
- NgayThangTransformer.js
- TrangThaiTransformer.js
- GiaTriTransformer.js

**Lưu ý:** Code framework, extractors, engines đã được generate sẵn. Các thành viên chỉ cần tập trung vào việc viết rules và transformers theo template đã có.

---

## 📝 Logs

### **log.txt** - Chi tiết quá trình
```
[2024-11-13 10:30:15] [INFO] EXTRACT - PostgreSQL Data Source 1 STARTED
[2024-11-13 10:30:16] [SUCCESS] Extracted 1000 records from khach_hang
...
```

### **errorLog.txt** - Các lỗi
```
[2024-11-13 10:35:20] [ERROR] Validation failed for record KH0001
{
  "field": "ho_ten",
  "error": "Họ tên chứa số",
  "value": "Nguyen Van 123"
}
...
```

---

## 🚀 Nâng cao

### Thêm Rule mới:

1. Tạo file `src/validation/rules/TenField.rule.js`
2. Implement theo template HoTen.rule.js
3. RuleEngine sẽ tự động load

### Thêm Transformer mới:

1. Tạo file `src/transform/transformers/TenFieldTransformer.js`
2. Implement theo template HoTenTransformer.js
3. TransformEngine sẽ tự động load

---

## 📧 Liên hệ

Nhóm 5 sinh viên - Dự án Tích hợp Dữ liệu

---

## 📄 License

MIT License