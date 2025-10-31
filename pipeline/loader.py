# scripts/loader.py
import sqlalchemy
import pandas as pd
import os  # <-- Thêm thư viện 'os' để xóa file
# Dòng code đúng
from pipeline.config import DATABASE_URL, DATASET_DIR, ARCHIVE_DIR

engine = sqlalchemy.create_engine(DATABASE_URL)

def load_data_to_postgres(df: pd.DataFrame, table_name: str, schema: str):
    """Nạp một DataFrame (đã có sẵn) vào bảng PostgreSQL được chỉ định."""
    if df.empty:
        print(f"Không có dữ liệu (DataFrame rỗng) để nạp vào {schema}.{table_name}.")
        return
        
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',  # Thêm dữ liệu mới vào bảng đã có
            index=False
        )
        print(f"-> Thành công: Đã nạp {len(df)} dòng (từ DataFrame) vào '{schema}.{table_name}'.")
    except Exception as e:
        print(f"-> LỖI khi nạp dữ liệu (từ DataFrame) vào {schema}.{table_name}: {e}")

# --- HÀM MỚI ĐƯỢC THÊM VÀO ---
def load_csv_to_staging_and_cleanup(file_path: str, schema: str = 'staging', table_name: str = 'raw_jobs'):
    """
    Hàm này thực hiện 3 bước:
    1. Đọc file CSV từ đường dẫn (file_path).
    2. Nạp (Load) dữ liệu vào bảng staging.
    3. Nếu nạp thành công, xóa file CSV gốc đi.
    """
    print(f"🔧 Bắt đầu quá trình nạp file: {file_path}")
    
    # --- Bước 1: Đọc file CSV ---
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"File {file_path} rỗng, không có gì để nạp. Bỏ qua.")
            # Xóa file rỗng nếu muốn
            # os.remove(file_path)
            # print(f"-> Đã xóa file rỗng: {file_path}")
            return
    except FileNotFoundError:
        print(f"❌ LỖI: Không tìm thấy file {file_path}.")
        return
    except Exception as e:
        print(f"❌ LỖI khi đọc file {file_path}: {e}")
        return

    # --- Bước 2: Nạp (Load) vào Database ---
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"✅ Thành công: Đã nạp {len(df)} dòng từ CSV vào bảng '{schema}.{table_name}'.")
        
        # --- Bước 3: Xóa file CSV (Chỉ chạy khi Bước 2 thành công) ---
        try:
            os.remove(file_path)
            print(f"🧹 Dọn dẹp: Đã xóa file {file_path}.")
        except Exception as e:
            print(f"⚠️ LỖI DỌN DẸP: Đã nạp DB thành công nhưng không thể xóa file {file_path}: {e}")

    except Exception as e:
        print(f"❌ LỖI NẠP DATABASE: {e}")
        print(f"File {file_path} SẼ ĐƯỢC GIỮ LẠI để kiểm tra (không bị xóa).")