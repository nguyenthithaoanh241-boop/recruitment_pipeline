# scripts/local_pusher_final.py
# CHẠY SCRIPT NÀY TRÊN MÁY LOCAL CỦA BẠN

import sqlalchemy
from sqlalchemy import text
import pandas as pd
import os 
import glob 
import sys 
from pipeline.config import DATASET_DIR, ARCHIVE_DIR, LOCAL_MYSQL_URL, REMOTE_SERVER_URL

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# --- CẤU HÌNH KẾT NỐI 
LOCAL_BACKUP_TABLE = "raw_jobs"
LOCAL_SCHEMA = "recruitment" 
REMOTE_STAGING_TABLE = "Stg_Jobs_ta"
REMOTE_SCHEMA = "dbo"

# --- CẤU HÌNH THƯ MỤC VÀ CỘT ---
LOCAL_DATASET_DIR = DATASET_DIR
LOCAL_ARCHIVE_DIR = ARCHIVE_DIR

STAGING_TABLE_COLUMNS = [ 
    "CongViec", "ViTri", "YeuCauKinhNghiem", "MucLuong",
    "CapBac", "HinhThucLamViec", "CongTy",
    "QuyMoCongTy", "SoLuongTuyen", "HocVan",
    "YeuCauUngVien", "MoTaCongViec", "QuyenLoi", "HanNopHoSo", 
    "LinkBaiTuyenDung", "Nguon", "NgayCaoDuLieu", "LinhVuc"
]
ALL_TEXT_COLS = [ 
     "CongViec", "ViTri", "YeuCauKinhNghiem", "MucLuong", "CapBac", "HinhThucLamViec", "CongTy",
     "QuyMoCongTy", "SoLuongTuyen", "HocVan", "YeuCauUngVien", "MoTaCongViec", 
     "QuyenLoi", "HanNopHoSo", "LinkBaiTuyenDung", "Nguon", "LinhVuc",
     "GioiTinh", "ThoiGianLamViec", "LinkCongTy", "ChuyenMon" 
]
# =================================================================

try:
    # Tạo 2 engine
    local_engine = sqlalchemy.create_engine(LOCAL_MYSQL_URL)
    server_engine = sqlalchemy.create_engine(REMOTE_SERVER_URL, fast_executemany=True)
    print("Da ket noi thanh cong voi 2 database (Local & Server).")
except Exception as e:
    print(f"LOI KET NOI DATABASE: {e}")
    sys.exit(1)


def push_data_from_local_to_server(csv_output_dir: str):
    """
    Doc CSV tu local, Backup vao Local DB, Push (Append) vao Server Staging
    """
    print(f"\n--- Bat dau Quet va Nap du lieu tu thu muc: {csv_output_dir} ---")
    
    os.makedirs(LOCAL_ARCHIVE_DIR, exist_ok=True)
    
    search_path = os.path.join(csv_output_dir, "*_jobs_*.csv")
    csv_files = glob.glob(search_path)

    if not csv_files:
        print(f"Khong tim thay file CSV nao (khop voi {search_path}).")
        return 0

    print(f"Tim thay {len(csv_files)} file CSV can load.")
    print(f"-> Backup (Local):  '{LOCAL_BACKUP_TABLE}'")
    print(f"-> Staging (Server): '{REMOTE_SCHEMA}.{REMOTE_STAGING_TABLE}'")
    
    total_rows_loaded = 0
    all_dfs = [] 
    
    full_dtype_map_sqlserver = {col: sqlalchemy.types.NVARCHAR() for col in ALL_TEXT_COLS}
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        archive_path = os.path.join(LOCAL_ARCHIVE_DIR, file_name)
        
        print("-" * 20)
        print(f"Dang xu ly file: {file_name}")

        # --- Buoc 1: Doc file CSV ---
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str) 
            if df.empty:
                print(f"File {file_name} rong, bo qua.")
                os.rename(file_path, archive_path)
                continue
        except Exception as e:
            print(f"LOI khi doc file {file_name}: {e}. Bo qua file nay.")
            continue 

        # --- Buoc 2: Nap (Load) vao BACKUP (Local MySQL) ---
        try:
            df.to_sql(
                name=LOCAL_BACKUP_TABLE,
                con=local_engine,
                schema=LOCAL_SCHEMA,
                if_exists='append', 
                index=False
            )
            rows = len(df)
            total_rows_loaded += rows
            print(f"Thanh cong (BACKUP LOCAL): Da nap {rows} dong vao bang '{LOCAL_BACKUP_TABLE}'.")
            
            # --- Buoc 3: Gom vao list va Di chuyen file ---
            all_dfs.append(df)
            
            try:
                os.rename(file_path, archive_path)
                print(f"Don dep: Da chuyen file {file_name} vao thu muc luu tru.")
            except Exception as e:
                print(f"LOI DON DEP: {e}")

        except Exception as e:
            print(f"LOI NAP DATABASE (BACKUP LOCAL) cho file {file_name}: {e}")
            print(f"File {file_name} SE DUOC GIU LAI de kiem tra.")
            
    # --- Buoc 4: Nap (Load) vao STAGING (Server Ảo) ---
    if not all_dfs:
        print("\nKhong co du lieu nao duoc gom de nap vao Staging.")
        return total_rows_loaded

    try:
        print("\nDang gom tat ca du lieu de chuan bi nap Staging...")
        df_all_new_data = pd.concat(all_dfs, ignore_index=True)
        
        # LỌC DataFrame chỉ giữ lại các cột mà Staging có
        print(f"Loc DataFrame de khop voi bang {REMOTE_STAGING_TABLE}...")
        
        valid_staging_cols = [col for col in STAGING_TABLE_COLUMNS if col in df_all_new_data.columns]
        df_for_staging = df_all_new_data[valid_staging_cols]
        
        # Lọc lại dtype_map cho Staging
        staging_dtype_map = {k: v for k, v in full_dtype_map_sqlserver.items() if k in valid_staging_cols}

        # <<< THAY ĐỔI: Đã xóa 'TRUNCATE' và 'with engine.begin()' >>>
        
        # APPEND (Nạp) dữ liệu đã lọc vào Server
        print(f"Dang ghi {len(df_for_staging)} dong vao STAGING tren SERVER (Append)...")
        df_for_staging.to_sql(
            name=REMOTE_STAGING_TABLE,
            con=server_engine, # Dùng 'server_engine'
            schema=REMOTE_SCHEMA,
            if_exists='append', # <<< DÙNG 'append' THEO YÊU CẦU >>>
            index=False,
            dtype=staging_dtype_map # Dùng map đã lọc
        )
        
        print("-> Ghi STAGING len SERVER thanh cong.")
        
    except Exception as e:
        print(f"LOI NAP DATABASE (STAGING SERVER): {e}")

    return total_rows_loaded

# =================================================================
if __name__ == "__main__":
    
    print(f"--- BAT DAU LOCAL PUSHER ---")
    print(f"Thu muc nguon (Local): {LOCAL_DATASET_DIR}")
    print(f"Bang Backup (Local):  {LOCAL_BACKUP_TABLE}")
    print(f"Bang Staging (Server): {REMOTE_SCHEMA}.{REMOTE_STAGING_TABLE}")
    print("-" * 40)

    try:
        total = push_data_from_local_to_server(
            csv_output_dir=LOCAL_DATASET_DIR
        )
        print(f"\n--- KET THUC: Da nap tong cong {total} dong vao Backup Local. ---")
    except Exception as e:
        print(f"\n--- LOI TEST NGHIEM TRONG: {e} ---")