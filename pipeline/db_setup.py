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
    
    # --- Tạo Schemas (Cú pháp PostgreSQL) ---
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS production;"))
    print("     -> Schemas 'staging' và 'production' đã sẵn sàng.")

    # --- Tạo bảng Staging (Cú pháp PostgreSQL) ---
    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS staging.raw_jobs (
        id SERIAL PRIMARY KEY,
        title TEXT,
        specialization TEXT,
        work_location TEXT,
        experience TEXT,
        salary TEXT,
        work_time TEXT,
        level TEXT,
        work_form TEXT,
        company_name TEXT,
        company_link TEXT,
        company_size TEXT,
        career_field TEXT,
        recruit_quantity TEXT,
        education TEXT,
        requirement TEXT,
        job_description TEXT,
        benefits TEXT,
        deadline TEXT,
        link TEXT UNIQUE, 
        gender TEXT,
        skills TEXT,
        post_date TEXT, 
        age TEXT,
        source_web TEXT,
        scraped_at TIMESTAMP WITH TIME ZONE,
        transform_status SMALLINT DEFAULT 0 NOT NULL, 
        loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """))
    print("     -> Bảng 'staging.raw_jobs' đã sẵn sàng.")

    # --- Tạo bảng Production (Cú pháp PostgreSQL) ---
    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS production.clean_jobs (
        id SERIAL PRIMARY KEY,
        job_id TEXT,
        title TEXT,
        company TEXT,
        salary_min NUMERIC,
        salary_max NUMERIC,
        currency VARCHAR(10),
        location TEXT,
        experience_years_min INT,
        level TEXT,
        skills TEXT[], -- Lưu skills dưới dạng mảng text
        post_date DATE,
        deadline DATE,
        source_web TEXT,
        link TEXT UNIQUE,
        transformed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """))
    print("     -> Bảng 'production.clean_jobs' đã sẵn sàng.")

def _setup_sqlserver(connection):
    """Tạo bảng và schema cho SQL Server."""
    print("🔧 Bắt đầu thiết lập cấu trúc cho SQL Server...")

    # --- Tạo Schemas (Cú pháp SQL Server) ---
    # Cú pháp này an toàn, chỉ tạo nếu chưa tồn tại
    connection.execute(text("""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    BEGIN
        EXEC('CREATE SCHEMA staging')
    END
    """))
    connection.execute(text("""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'production')
    BEGIN
        EXEC('CREATE SCHEMA production')
    END
    """))
    print("     -> Schemas 'staging' và 'production' đã sẵn sàng.")

    # --- Tạo bảng Staging (Cú pháp SQL Server) ---
    connection.execute(text("""
    IF OBJECT_ID('staging.raw_jobs', 'U') IS NULL
    BEGIN
        CREATE TABLE staging.raw_jobs (
            id INT IDENTITY(1,1) PRIMARY KEY, -- Thay SERIAL
            title NVARCHAR(MAX), -- Thay TEXT
            specialization NVARCHAR(MAX),
            work_location NVARCHAR(MAX),
            experience NVARCHAR(MAX),
            salary NVARCHAR(MAX),
            work_time NVARCHAR(MAX),
            level NVARCHAR(MAX),
            work_form NVARCHAR(MAX),
            company_name NVARCHAR(MAX),
            company_link NVARCHAR(MAX),
            company_size NVARCHAR(MAX),
            career_field NVARCHAR(MAX),
            recruit_quantity NVARCHAR(MAX),
            education NVARCHAR(MAX),
            requirement NVARCHAR(MAX),
            job_description NVARCHAR(MAX),
            benefits NVARCHAR(MAX),
            deadline NVARCHAR(MAX),
            link NVARCHAR(450) UNIQUE, -- Giới hạn cho UNIQUE, thay TEXT
            gender NVARCHAR(MAX),
            skills NVARCHAR(MAX),
            post_date NVARCHAR(MAX), 
            age NVARCHAR(MAX),
            source_web NVARCHAR(MAX),
            scraped_at DATETIMEOFFSET, -- Thay TIMESTAMP WITH TIME ZONE
            transform_status SMALLINT DEFAULT 0 NOT NULL, 
            loaded_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET() -- Thay NOW()
        );
    END
    """))
    print("     -> Bảng 'staging.raw_jobs' đã sẵn sàng.")

    # --- Tạo bảng Production (Cú pháp SQL Server) ---
    connection.execute(text("""
    IF OBJECT_ID('production.clean_jobs', 'U') IS NULL
    BEGIN
        CREATE TABLE production.clean_jobs (
            id INT IDENTITY(1,1) PRIMARY KEY, -- Thay SERIAL
            job_id NVARCHAR(MAX),
            title NVARCHAR(MAX),
            company NVARCHAR(MAX),
            salary_min NUMERIC,
            salary_max NUMERIC,
            currency VARCHAR(10),
            location NVARCHAR(MAX),
            experience_years_min INT,
            level NVARCHAR(MAX),
            skills NVARCHAR(MAX), -- Thay TEXT[], lưu dạng JSON hoặc CSV
            post_date DATE,
            deadline DATE,
            source_web NVARCHAR(MAX),
            link NVARCHAR(450) UNIQUE, -- Giới hạn cho UNIQUE, thay TEXT
            transformed_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET() -- Thay NOW()
        );
    END
    """))
    print("     -> Bảng 'production.clean_jobs' đã sẵn sàng.")


def setup_database_tables():
    """
    Hàm này tạo các schema và bảng cần thiết cho pipeline.
    Nó sẽ tự động gọi hàm setup cho đúng loại database.
    """
    try:
        with engine.connect() as connection:
            # Bắt đầu một transaction
            with connection.begin() as transaction:
                
                # Kiểm tra DB_TYPE và gọi hàm tương ứng
                if DB_TYPE == "postgresql":
                    _setup_postgresql(connection)
                elif DB_TYPE == "sqlserver":
                    _setup_sqlserver(connection)
                else:
                    raise ValueError(f"DB_TYPE '{DB_TYPE}' không được hỗ trợ.")
                
                # transaction.commit() sẽ được gọi tự động khi khối 'with' kết thúc
            
            print("✅ Hoàn tất thiết lập database!")

    except ProgrammingError as e:
        # Cập nhật logic bắt lỗi cho cả hai
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