// constants.js - Chứa tất cả các giá trị enum và constant

module.exports = {
  // Trạng thái đơn hàng
  TRANG_THAI_DON_HANG: {
    CHO_XU_LY: 'Chờ xử lý',
    DANG_CHUAN_BI: 'Đang chuẩn bị',
    DANG_GIAO: 'Đang giao',
    DA_GIAO: 'Đã giao',
    HUY: 'Hủy',
    TRA_HANG: 'Trả hàng',
    DANG_XU_LY: 'Đang xử lý' 
  },

  // Trạng thái thanh toán
  TRANG_THAI_THANH_TOAN: {
    CHUA_THANH_TOAN: 'Chưa thanh toán',
    DA_THANH_TOAN: 'Đã thanh toán',
    HOAN_TIEN: 'Hoàn tiền',
    THAT_BAI: 'Thất bại',
    THANH_CONG: 'Thành công', // <--- THÊM DÒNG NÀY
    DANG_XU_LY: 'Đang xử lý'  // <--- THÊM DÒNG NÀY
  },

  // Phương thức thanh toán
  PHUONG_THUC_THANH_TOAN: {
    TIEN_MAT: 'Tiền mặt',
    CHUYEN_KHOAN: 'Chuyển khoản',
    THE_TIN_DUNG: 'Thẻ tín dụng',
    VI_DIEN_TU: 'Ví điện tử',
    COD: 'COD'
  },

  // Giới tính
  GIOI_TINH: {
    NAM: 'Nam',
    NU: 'Nữ',
    KHAC: 'Khác'
  },

  // Loại khách hàng
  LOAI_KHACH_HANG: {
    THUONG: 'Thường',
    VIP: 'VIP',
    DOANH_NGHIEP: 'Doanh nghiệp',
    MOI: 'Mới'
  },

  // Trạng thái phiếu hỗ trợ
  TRANG_THAI_PHIEU_HO_TRO: {
    MOI: 'Mới',
    DANG_XU_LY: 'Đang xử lý',
    CHO_PHAN_HOI: 'Chờ phản hồi',
    DA_GIAI_QUYET: 'Đã giải quyết',
    DONG: 'Đóng',
    HUY: 'Hủy'
  },

  // Độ ưu tiên
  DO_UU_TIEN: {
    THAP: 'Thấp',
    TRUNG_BINH: 'Trung bình',
    CAO: 'Cao',
    KHAN_CAP: 'Khẩn cấp'
  },

  // Trạng thái nhân viên
  TRANG_THAI_NHAN_VIEN: {
    DANG_LAM: 'Đang làm',
    NGHI_PHEP: 'Nghỉ phép',
    DA_NGHI: 'Đã nghỉ'
  },

  // Chức vụ
  CHUC_VU: {
    NHAN_VIEN: 'Nhân viên',
    TRUONG_NHOM: 'Trưởng nhóm',
    QUAN_LY: 'Quản lý',
    GIAM_DOC: 'Giám đốc'
  },

  // Kết quả xử lý
  KET_QUA_XU_LY: {
    THANH_CONG: 'Thành công',
    THAT_BAI: 'Thất bại',
    DANG_XU_LY: 'Đang xử lý'
  },

  // Regex patterns
  REGEX: {
    EMAIL: /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/,
    PHONE: /^(0|\+84)[0-9]{9,10}$/,
    MA_KHACH_HANG: /^KH\d{4}$/,
    MA_DON_HANG: /^DH\d{4}$/,
    MA_SAN_PHAM: /^SP\d{4}$/,
    MA_DANH_MUC: /^DM\d{4}$/,
    MA_THANH_TOAN: /^TT\d{4}$/,
    MA_PHIEU_HO_TRO: /^PHT\d{4}$/,
    MA_DANH_GIA: /^DG\d{4}$/,
    MA_NHAN_VIEN: /^NV\d{4}$/,
    MA_PHIEU_XU_LY: /^PXL\d{4}$/
  },

  // Validation limits
  LIMITS: {
    MIN_AGE: 13,
    MAX_AGE: 120,
    MIN_PRICE: 0,
    MAX_PRICE: 999999999,
    MIN_QUANTITY: 1,
    MAX_QUANTITY: 10000,
    MIN_RATING: 1,
    MAX_RATING: 5,
    MAX_NAME_LENGTH: 100,
    MAX_EMAIL_LENGTH: 100,
    MAX_PHONE_LENGTH: 20
  },

  // Error codes
  ERROR_CODES: {
    INVALID_FORMAT: 'E001',
    NULL_VALUE: 'E002',
    OUT_OF_RANGE: 'E003',
    DUPLICATE: 'E004',
    INVALID_REFERENCE: 'E005',
    INVALID_DATE: 'E006',
    INVALID_STATUS: 'E007'
  },

  // Transform actions
  TRANSFORM_ACTIONS: {
    UPPERCASE: 'uppercase',
    LOWERCASE: 'lowercase',
    TRIM: 'trim',
    CAPITALIZE: 'capitalize',
    NORMALIZE_VIETNAMESE: 'normalize_vietnamese',
    FORMAT_DATE: 'format_date',
    FORMAT_PHONE: 'format_phone',
    FORMAT_PRICE: 'format_price'
  }
};