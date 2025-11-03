# scripts/loader.py

import sqlalchemy
import pandas as pd
import os 
import glob # ĐÃ THÊM glob
# Dòng code đúng
from pipeline.config import DATABASE_URL, DATASET_DIR, ARCHIVE_DIR

engine = sqlalchemy.create_engine(DATABASE_URL)

def load_df_to_db(df: pd.DataFrame, table_name: str, schema: str):
    """Nạp một DataFrame (đã có sẵn) vào bảng được chỉ định."""
    if df.empty:
        print(f"Không có dữ liệu (DataFrame rỗng) để nạp vào {schema}.{table_name}.")
        return
        
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"-> Thành công: Đã nạp {len(df)} dòng (từ DataFrame) vào '{schema}.{table_name}'.")
    except Exception as e:
        print(f"-> LỖI khi nạp dữ liệu (từ DataFrame) vào {schema}.{table_name}: {e}")

# --- HÀM MỚI QUÉT VÀ LOAD TẤT CẢ FILE CSV ---
def load_all_csv_to_staging_and_cleanup(csv_output_dir: str, schema: str, table_name: str):
    """
    Tìm tất cả các file CSV trong thư mục, nạp từng file vào database, 
    và xóa file sau khi nạp thành công.
    """
    print(f"\n--- Bắt đầu Quét và Nạp dữ liệu từ thư mục: {csv_output_dir} ---")
    
    # Sử dụng glob để tìm tất cả các file CSV có pattern tên phù hợp (*_jobs_*.csv)
    search_path = os.path.join(csv_output_dir, "*_jobs_*.csv")
    csv_files = glob.glob(search_path)

    if not csv_files:
        print("📦 Không tìm thấy file CSV mới nào (hoặc file cũ chưa xử lý) để load.")
        return 0

    print(f"📁 Tìm thấy {len(csv_files)} file CSV cần load vào '{schema}.{table_name}'.")
    
    total_rows_loaded = 0
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print("-" * 20)
        print(f"🔧 Đang xử lý file: {file_name}")

        # --- Bước 1: Đọc file CSV ---
        try:
            # Đọc file CSV với encoding phù hợp
            df = pd.read_csv(file_path, encoding='utf-8-sig') 
            
            if df.empty:
                print(f"File {file_name} rỗng, không có gì để nạp. Xóa file rỗng.")
                os.remove(file_path)
                continue
        except Exception as e:
            print(f"❌ LỖI khi đọc file {file_name}: {e}. Bỏ qua file này.")
            continue # Chuyển sang file tiếp theo

        # --- Bước 2: Nạp (Load) vào Database ---
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='append',
                index=False
            )
            rows = len(df)
            total_rows_loaded += rows
            print(f"✅ Thành công: Đã nạp {rows} dòng từ CSV vào bảng '{schema}.{table_name}'.")
            
            # --- Bước 3: Xóa file CSV (Chỉ chạy khi Bước 2 thành công) ---
            try:
                os.remove(file_path)
                print(f"🧹 Dọn dẹp: Đã xóa file {file_name}.")
            except Exception as e:
                print(f"⚠️ LỖI DỌN DẸP: Đã nạp DB thành công nhưng không thể xóa file {file_name}: {e}")

        except Exception as e:
            print(f"❌ LỖI NẠP DATABASE cho file {file_name}: {e}")
            print(f"File {file_name} SẼ ĐƯỢC GIỮ LẠI để kiểm tra (không bị xóa).")
            
    return total_rows_loaded