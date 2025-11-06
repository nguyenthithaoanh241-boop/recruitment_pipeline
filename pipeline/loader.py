# scripts/loader.py

import sqlalchemy
import pandas as pd
import os 
import glob 
import sys 


# Them project root (thu muc cha cua 'pipeline') vao sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
# Import cau hinh tu pipeline/config.py
from pipeline.config import DATABASE_URL, DATASET_DIR, ARCHIVE_DIR

engine = sqlalchemy.create_engine(DATABASE_URL)

def load_df_to_db(df: pd.DataFrame, table_name: str, schema: str):
    """Nap mot DataFrame co san vao bang duoc chi dinh."""
    if df.empty:
        print(f"Khong co du lieu (DataFrame rong) de nap vao {schema}.{table_name}.")
        return
        
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"-> Thanh cong: Da nap {len(df)} dong (tu DataFrame) vao '{schema}.{table_name}'.")
    except Exception as e:
        print(f"-> LOI khi nap du lieu (tu DataFrame) vao {schema}.{table_name}: {e}")

def load_all_csv_to_staging_and_cleanup(csv_output_dir: str, schema: str, table_name: str):
    """
    Tim tat ca file CSV, nap vao database, 
    va di chuyen file vao thu muc luu tru (archive) sau khi nap.
    """
    print(f"\n--- Bat dau Quet va Nap du lieu tu thu muc: {csv_output_dir} ---")
    
    # Dam bao thu muc luu tru ton tai
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Tim tat ca file CSV co pattern *_jobs_*.csv
    search_path = os.path.join(csv_output_dir, "*_jobs_*.csv")
    csv_files = glob.glob(search_path)

    if not csv_files:
        print("Khong tim thay file CSV moi nao de load.")
        return 0

    print(f"Tim thay {len(csv_files)} file CSV can load vao '{schema}.{table_name}'.")
    
    total_rows_loaded = 0
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        archive_path = os.path.join(ARCHIVE_DIR, file_name) # Dinh nghia duong dan luu tru
        
        print("-" * 20)
        print(f"Dang xu ly file: {file_name}")

        # --- Buoc 1: Doc file CSV ---
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig') 
            
            if df.empty:
                print(f"File {file_name} rong, khong co gi de nap. Chuyen vao luu tru.")
                # Dung os.rename thay vi os.remove
                os.rename(file_path, archive_path)
                continue
        except Exception as e:
            print(f"LOI khi doc file {file_name}: {e}. Bo qua file nay.")
            continue 

        # --- Buoc 2: Nap (Load) vao Database ---
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
            print(f"Thanh cong: Da nap {rows} dong tu CSV vao bang '{schema}.{table_name}'.")
            
            # --- Buoc 3: Di chuyen file CSV (Chi chay khi Buoc 2 thanh cong) ---
            try:
                # Dung os.rename thay vi os.remove
                os.rename(file_path, archive_path)
                print(f"Don dep: Da chuyen file {file_name} vao thu muc luu tru.")
            except Exception as e:
                print(f"LOI DON DEP: Da nap DB thanh cong nhung khong thể di chuyen file {file_name}: {e}")

        except Exception as e:
            print(f"LOI NAP DATABASE cho file {file_name}: {e}")
            print(f"File {file_name} SE DUOC GIU LAI de kiem tra (khong bi xoa/di chuyen).")
            
    return total_rows_loaded

# =================================================================
# Khoi code de chay file doc lap
# =================================================================
if __name__ == "__main__":
    
    # 1. Them project root vao sys.path de import 'pipeline.config'
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)
    
    # 2. Import them DB_TYPE va DB_NAME de xac dinh schema
    try:
        from pipeline.config import DB_TYPE, DB_NAME
    except ImportError:
        print("Loi: Khong the import DB_TYPE, DB_NAME tu pipeline.config.")
        print("Vui long kiem tra file pipeline/config.py.")
        sys.exit(1) # Thoat neu khong co config

    # 3. Xac dinh Schema va Ten Bang
    TABLE_NAME = "raw_jobs" # Ten bang chung (MySQL)
    SCHEMA_NAME = None

    if DB_TYPE == "postgresql":
        SCHEMA_NAME = "staging"
    elif DB_TYPE == "sqlserver":
        SCHEMA_NAME = "dbo"
        TABLE_NAME = "raw_jobs_ta" # SQL Server dung ten bang khac
    elif DB_TYPE == "mysql":
        SCHEMA_NAME = DB_NAME # Voi MySQL, schema chinh la ten database
    else:
        print(f"Canh bao: DB_TYPE '{DB_TYPE}' khong xac dinh. Dat schema=None.")

    print(f"--- BAT DAU CHAY LOADER (TEST) ---")
    print(f"Database Type: {DB_TYPE}")
    print(f"Thu muc nguon: {DATASET_DIR}")
    print(f"Bang dich:     {SCHEMA_NAME}.{TABLE_NAME}")
    print(f"Luu tru tai:   {ARCHIVE_DIR}")
    print("-" * 40)

    # 4. Goi ham load
    try:
        total = load_all_csv_to_staging_and_cleanup(
            csv_output_dir=DATASET_DIR,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME
        )
        print(f"\n--- KET THUC TEST: Da nap tong cong {total} dong. ---")
    except Exception as e:
        print(f"\n--- LOI TEST NGHIEM TRONG: {e} ---")