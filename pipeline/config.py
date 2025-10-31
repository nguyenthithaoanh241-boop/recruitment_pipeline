# pipeline/config.py (Phiên bản nâng cấp)

import os
from dotenv import load_dotenv
from urllib.parse import quote_plus # <-- Thêm thư viện này để xử lý mật khẩu

# --- Cấu hình đường dẫn (Giữ nguyên) ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOTENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(DOTENV_PATH)

print("✅ Đã load file .env")

# --- Đọc cấu hình từ .env ---
DB_TYPE = os.getenv("DB_TYPE")     # <-- Biến mới quan trọng
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# --- Biến DATABASE_URL sẽ được tạo tự động ---
DATABASE_URL = None

if DB_TYPE == "postgresql":
    # Tạo chuỗi cho PostgreSQL (như cũ)
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("✅ Đã cấu hình cho PostgreSQL")

elif DB_TYPE == "sqlserver":
    # Lấy tên driver cho SQL Server từ .env
    DB_DRIVER = os.getenv("DB_DRIVER")
    
    # Mã hóa mật khẩu (quan trọng nếu mật khẩu có ký tự đặc biệt)
    safe_password = quote_plus(DB_PASSWORD)
    
    # Tạo chuỗi cho SQL Server
    DATABASE_URL = f"mssql+pyodbc://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver={DB_DRIVER}"
    print("✅ Đã cấu hình cho SQL Server")

else:
    print(f"LỖI: DB_TYPE '{DB_TYPE}' không được hỗ trợ hoặc chưa được set trong .env")


# --- Cấu hình đường dẫn (Giữ nguyên) ---
DATASET_DIR = os.path.join(BASE_DIR, 'dataset')
ARCHIVE_DIR = os.path.join(DATASET_DIR, 'archive') 

# --- Kiểm tra ---
print(f"💡 DB_USER được sử dụng là: {DB_USER}")
# print(f"💡 DATABASE_URL được tạo: {DATABASE_URL}") # Bỏ comment nếu muốn debug