# pipeline/config.py

import os
from dotenv import load_dotenv
from urllib.parse import quote_plus # Dùng để xử lý mật khẩu có ký tự đặc biệt

# --- Cấu hình đường dẫn ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOTENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(DOTENV_PATH)

print("Da load file .env")

# --- Đọc cấu hình database từ .env ---
DB_TYPE = os.getenv("DB_TYPE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# --- Xây dựng DATABASE_URL ---
DATABASE_URL = None

if DB_TYPE == "postgresql":
    # Thêm +psycopg2 để chỉ rõ driver
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("Da cau hinh cho PostgreSQL")

elif DB_TYPE == "sqlserver":
    DB_DRIVER = os.getenv("DB_DRIVER")
    safe_password = quote_plus(DB_PASSWORD)
    DATABASE_URL = f"mssql+pyodbc://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver={DB_DRIVER}"
    print("Da cau hinh cho SQL Server")
    
elif DB_TYPE == "mysql":
    safe_password = quote_plus(DB_PASSWORD)
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("Da cau hinh cho MySQL (dung pymysql)")
    
else:
    print(f"LOI: DB_TYPE '{DB_TYPE}' khong duoc ho tro hoac chua duoc set trong .env")


# --- Cấu hình đường dẫn thư mục ---
DATASET_DIR = os.path.join(BASE_DIR, 'dataset')
ARCHIVE_DIR = os.path.join(DATASET_DIR, 'archive') 

# --- Kiểm tra (chạy khi file được import hoặc chạy trực tiếp) ---
print(f"DB_USER duoc su dung la: {DB_USER}")
print(f"DATABASE_URL duoc tao: {DATABASE_URL}")