# pipeline/db_setup.py 
import sqlalchemy
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text 
from pipeline.config import DATABASE_URL, DB_NAME, DB_TYPE 

try:
    engine = sqlalchemy.create_engine(DATABASE_URL)
    print(f"✅ Kết nối database thành công (Loại: {DB_TYPE})!")
except Exception as e:
    print(f"❌ Lỗi kết nối database: {e}")
    exit()

def _setup_postgresql(connection):
    """Tạo bảng và schema cho PostgreSQL."""
    print("🔧 Bắt đầu thiết lập cấu trúc cho PostgreSQL...")
    
    # --- Tạo Schema 'staging' (nếu chưa có) ---
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    print("     -> Schema 'staging' đã sẵn sàng.")

    # --- Tạo bảng Staging (Cú pháp PostgreSQL) ---
    # Sử dụng các cột Tiếng Việt từ CSV_HEADER
    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS staging.raw_jobs_ta (
        id SERIAL PRIMARY KEY,
        CongViec TEXT,
        ChuyenMon TEXT,
        ViTri TEXT,
        YeuCauKinhNghiem TEXT,
        MucLuong TEXT,
        ThoiGianLamViec TEXT,
        CapBac TEXT,
        HinhThucLamViec TEXT,
        CongTy TEXT,
        LinkCongTy TEXT,
        QuyMoCongTy TEXT,
        SoLuongTuyen TEXT,
        HocVan TEXT,
        YeuCauUngVien TEXT,
        MoTaCongViec TEXT,
        QuyenLoi TEXT,
        HanNopHoSo TEXT,
        LinkBaiTuyenDung TEXT,
        Nguon TEXT,
        NgayCaoDuLieu DATE,
        
        -- Cột metadata (để theo dõi)
        NgayThemVaoHeThong TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        -- Thêm UNIQUE constraint để tránh trùng lặp job
        CONSTRAINT unique_link_pg UNIQUE (LinkBaiTuyenDung)
    );
    """))
    print("     -> Bảng 'staging.raw_jobs_ta' (Tiếng Việt) đã sẵn sàng.")


def _setup_sqlserver(connection):
    """Tạo bảng và schema cho SQL Server."""
    print("🔧 Bắt đầu thiết lập cấu trúc cho SQL Server...")

    # --- Tạo Schema 'dbo' (mặc định) ---
    # (Chúng ta sẽ tạo bảng trong 'dbo' để khớp với main.py và ảnh của bạn)
    print("     -> Sẽ sử dụng schema 'dbo' mặc định.")

    # --- Tạo bảng Staging (Cú pháp SQL Server) ---
    # Sử dụng các cột Tiếng Việt từ CSV_HEADER
    connection.execute(text("""
    IF OBJECT_ID('dbo.raw_jobs_ta', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.raw_jobs_ta (
            id INT IDENTITY(1,1) PRIMARY KEY,
            CongViec NVARCHAR(MAX),
            ChuyenMon NVARCHAR(MAX),
            ViTri NVARCHAR(MAX),
            YeuCauKinhNghiem NVARCHAR(MAX),
            MucLuong NVARCHAR(MAX),
            ThoiGianLamViec NVARCHAR(MAX),
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
            LinkBaiTuyenDung NVARCHAR(450), -- 450 là giới hạn để dùng UNIQUE
            Nguon NVARCHAR(255),
            NgayCaoDuLieu DATE,
            
            -- Cột metadata (để theo dõi)
            NgayThemVaoHeThong DATETIME DEFAULT GETDATE(),
            -- Thêm UNIQUE constraint để tránh trùng lặp job
            CONSTRAINT unique_link_sql UNIQUE (LinkBaiTuyenDung)
        );
    END
    """))
    print("     -> Bảng 'dbo.raw_jobs_ta' (Tiếng Việt) đã sẵn sàng.")


def setup_database_tables():
    """
    Hàm này tạo các schema và bảng cần thiết cho pipeline.
    Nó sẽ tự động gọi hàm setup cho đúng loại database.
    """
    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                
                if DB_TYPE == "postgresql":
                    _setup_postgresql(connection)
                elif DB_TYPE == "sqlserver":
                    _setup_sqlserver(connection)
                else:
                    raise ValueError(f"DB_TYPE '{DB_TYPE}' không được hỗ trợ.")
                
            print("✅ Hoàn tất thiết lập database!")

    except ProgrammingError as e:
        if (("does not exist" in str(e).lower() or "cannot open database" in str(e).lower()) 
            and DB_NAME in str(e)):
            print(f"❌ Lỗi: Database '{DB_NAME}' không tồn tại.")
            print(f"     Vui lòng tạo database này trong {DB_TYPE} trước khi chạy pipeline.")
        else:
            print(f"❌ Đã xảy ra lỗi SQL: {e}")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi không xác định: {e}")

# Cho phép chạy file này độc lập để setup DB
if __name__ == "__main__":
    setup_database_tables()