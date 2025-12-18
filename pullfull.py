import sys
import os
import pandas as pd
import sqlalchemy
import pyodbc
import logging
from sqlalchemy import text
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote_plus

# ==============================================================================
# 1. CẤU HÌNH LOGGING
# ==============================================================================
def setup_logging(log_file_name="etl_full_sync.log"):
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file_name, mode='a', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

setup_logging()
logging.info("--- CHUONG TRINH PULL FULL DATA KHOI DONG ---")

# ==============================================================================
# 2. CẤU HÌNH KẾT NỐI
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
dotenv_path = os.path.join(project_root, '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logging.info("✅ Da load file .env")

# Config SQL Server (Nguồn)
DB_SERVER = os.getenv("DB_SERVER", "103.141.144.235")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "Tuyen_Dung_HN")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123")

# Config MySQL (Đích)
MYSQL_URL_ENV = os.getenv("LOCAL_MYSQL_URL", "mysql+pymysql://root:123456@localhost:3306/recruitment?charset=utf8mb4")

def get_best_driver():
    installed_drivers = pyodbc.drivers()
    preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]
    for d in preferred:
        if d in installed_drivers:
            return d
    raise Exception("❌ Khong tim thay Driver SQL Server.")

current_driver = get_best_driver()
sql_conn_str = f"DRIVER={{{current_driver}}};SERVER={DB_SERVER},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD};"
if "Driver 18" in current_driver or "Driver 17" in current_driver:
    sql_conn_str += "TrustServerCertificate=yes;Encrypt=yes;"

sql_server_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(sql_conn_str)}"

# ==============================================================================
# 3. LOGIC PULL FULL DATA
# ==============================================================================

def pull_full_data():
    logging.info(f"🚀 Bat dau lay toan bo du lieu tu: {DB_SERVER}")
    
    try:
        sql_source_engine = sqlalchemy.create_engine(sql_server_url)
        mysql_dest_engine = sqlalchemy.create_engine(MYSQL_URL_ENV)
        
        # 1. Dung SELECT * de lay het tat ca cac cot
        query = text("SELECT * FROM [dbo].[Fact_JobPostings]")
        
        logging.info("⏳ Dang doc du lieu tu SQL Server (Vui long cho)...")
        df = pd.read_sql(query, sql_source_engine)
        
        if df.empty:
            logging.warning("⚠️ Khong co du lieu de tai.")
            return

        logging.info(f"📦 Tong cong: {len(df)} dong du lieu.")

        # 2. Mapping kieu du lieu de tranh loi MySQL cat ngan chuoi (Varchar vs Text)
        object_cols = df.select_dtypes(include='object').columns
        dtype_map = {col: sqlalchemy.types.TEXT() for col in object_cols}

        # 3. Ghi vao MySQL - Tu dong tao bang moi hoac ghi de bang cu
        logging.info("📝 Dang ghi du lieu vao MySQL (Che do: REPLACE)...")
        
        with mysql_dest_engine.begin() as conn:
            df.to_sql(
                name='raw_jobs1', 
                con=conn,
                if_exists='replace', # Xoa bang cu (neu co), tao bang moi va chen data
                index=False,
                dtype=dtype_map,
                chunksize=1000 # Chia nho de day vao cho nhanh va on dinh
            )
            
        logging.info("✅ HOAN THANH: Toan bo du lieu da duoc sao chep sang MySQL.")

    except Exception as e:
        logging.error(f"❌ LOI: {e}")
    finally:
        logging.info("--- KET THUC QUY TRINH ---")

if __name__ == "__main__":
    pull_full_data()