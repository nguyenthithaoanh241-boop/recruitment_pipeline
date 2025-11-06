# pipeline/db_setup.py 
import sqlalchemy
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text 
import sys 
import os 

# Them thu muc goc vao sys.path de import config
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import config (phai nam sau khi sua sys.path)
from pipeline.config import DATABASE_URL, DB_NAME, DB_TYPE 

try:
    engine = sqlalchemy.create_engine(DATABASE_URL)
    print(f"Ket noi database thanh cong (Loai: {DB_TYPE})!")
except Exception as e:
    print(f"Loi ket noi database: {e}")
    exit()


def _setup_mysql(connection):
    """Tao bang 22 cot cho MySQL."""
    
    print("Bat dau thiet lap cau truc cho MySQL...")
    print(f"    -> Dang lam viec tren database: '{DB_NAME}'.")

    # Su dung LONGTEXT va utf8mb4 de ho tro tieng Viet
    connection.execute(text(f"""
    CREATE TABLE IF NOT EXISTS raw_jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        CongViec LONGTEXT,
        ChuyenMon LONGTEXT,
        ViTri LONGTEXT,
        YeuCauKinhNghiem LONGTEXT,
        MucLuong LONGTEXT,
        ThoiGianLamViec LONGTEXT,
        GioiTinh LONGTEXT, 
        CapBac LONGTEXT,
        HinhThucLamViec LONGTEXT,
        CongTy LONGTEXT,
        LinkCongTy LONGTEXT,
        QuyMoCongTy LONGTEXT,
        SoLuongTuyen LONGTEXT,
        HocVan LONGTEXT,
        YeuCauUngVien LONGTEXT,
        MoTaCongViec LONGTEXT,
        QuyenLoi LONGTEXT,
        HanNopHoSo LONGTEXT,
        LinkBaiTuyenDung LONGTEXT,
        Nguon LONGTEXT,
        NgayCaoDuLieu DATE,
        LinhVuc LONGTEXT 
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """))
    
    print(f"    -> Bang '{DB_NAME}.raw_jobs' (22 cot) da san sang.")


def _setup_sqlserver(connection):
    """Tao bang 22 cot cho SQL Server."""
    print("Bat dau thiet lap cau truc cho SQL Server...")
    print("    -> Se su dung schema 'dbo' mac dinh.")

    connection.execute(text("""
    IF OBJECT_ID('dbo.raw_jobs_ta', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.raw_jobs_ta (
            CongViec NVARCHAR(MAX),
            ChuyenMon NVARCHAR(MAX),
            ViTri NVARCHAR(MAX),
            YeuCauKinhNghiem NVARCHAR(MAX),
            MucLuong NVARCHAR(MAX),
            ThoiGianLamViec NVARCHAR(MAX),
            GioiTinh NVARCHAR(MAX),
            CapBac NVARCHAR(MAX),
            HinhThucLamViec NVARCHAR(MAX),
            CongTy NVARCHAR(MAX),
            LinkCongTy NVARCHAR(MAX),
            QuyMoCongTy NVARCHAR(MAX),
            SoLuongTuyen NVARCHAR(MAX),
            HocVan NVARCHAR(MAX),
            YeuCauUngVien NVARCHAR(MAX),
            MoTaCongViec NVARCHAR(MAX),
            QuyenLoi NVARCHAR(MAX),
            HanNopHoSo NVARCHAR(MAX),
            LinkBaiTuyenDung NVARCHAR(450), 
            Nguon NVARCHAR(255),
            NgayCaoDuLieu DATE,
            LinhVuc NVARCHAR(MAX) 
        );
    END
    """))
    print("    -> Bang 'dbo.raw_jobs_ta' (22 cot) da san sang.")


def setup_database_tables():
    """
    Tu dong goi ham setup cho dung loai database (DB_TYPE).
    """
    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                
                if DB_TYPE == "mysql":
                    _setup_mysql(connection)
                elif DB_TYPE == "sqlserver":
                    _setup_sqlserver(connection)
                else:
                    raise ValueError(f"DB_TYPE '{DB_TYPE}' khong duoc ho tro.")
                
            print("Hoan tat thiet lap database!")

    except ProgrammingError as e:
        # Bat loi neu database (schema) khong ton tai
        if (("does not exist" in str(e).lower() 
             or "cannot open database" in str(e).lower()
             or "unknown database" in str(e).lower()) 
            and DB_NAME in str(e)):
            print(f"Loi: Database '{DB_NAME}' khong ton tai.")
            print(f"    Vui long tao database nay trong {DB_TYPE} truoc khi chay pipeline.")
        else:
            print(f"Da xay ra loi SQL: {e}")
    except Exception as e:
        print(f"Da xay ra loi khong xac dinh: {e}")

if __name__ == "__main__":
    print("--- Bat dau chay DB Setup doc lap ---")
    setup_database_tables()
    print("--- Hoan tat chay DB Setup ---")