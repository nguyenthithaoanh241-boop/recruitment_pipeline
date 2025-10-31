# pipeline/db_setup.py

import sqlalchemy
from sqlalchemy.exc import ProgrammingError
from pipeline.config import DATABASE_URL, DB_NAME

# Tạo một database engine để kết nối
try:
    engine = sqlalchemy.create_engine(DATABASE_URL)
    print("✅ Kết nối database thành công!")
except Exception as e:
    print(f"❌ Lỗi kết nối database: {e}")
    exit()

def setup_database_tables():
    """
    Hàm này tạo các schema và bảng cần thiết cho pipeline.
    - staging: Chứa dữ liệu thô, chưa qua xử lý.
    - production: Chứa dữ liệu đã được làm sạch, sẵn sàng cho phân tích.
    """
    try:
        with engine.connect() as connection:
            print("🔧 Bắt đầu thiết lập cấu trúc database...")
            
            # --- Tạo Schemas ---
            connection.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS staging;"))
            connection.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS production;"))
            print("    -> Schemas 'staging' và 'production' đã sẵn sàng.")

            # --- Tạo bảng Staging (dữ liệu thô) ---
            # Thêm cột 'id' tự tăng và 'loaded_at' để theo dõi
            connection.execute(sqlalchemy.text("""
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
                age TEXt,
                source_web TEXT,
                scraped_at TIMESTAMP WITH TIME ZONE,
                transform_status SMALLINT DEFAULT 0 NOT NULL, 
                loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """))
            print("    -> Bảng 'staging.raw_jobs' đã sẵn sàng.")

            # --- Tạo bảng Production (dữ liệu sạch) ---
            connection.execute(sqlalchemy.text("""
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
            print("    -> Bảng 'production.clean_jobs' đã sẵn sàng.")

            # Commit tất cả các thay đổi
            connection.commit()
            print("✅ Hoàn tất thiết lập database!")

    except ProgrammingError as e:
        # Lỗi thường gặp nếu database chưa được tạo
        if DB_NAME in str(e) and "does not exist" in str(e):
            print(f"❌ Lỗi: Database '{DB_NAME}' không tồn tại.")
            print("    Vui lòng tạo database này trong PostgreSQL trước khi chạy pipeline.")
        else:
            print(f"❌ Đã xảy ra lỗi SQL: {e}")
    except Exception as e:
        print(f"❌ Đã xảy ra lỗi không xác định: {e}")

# Cho phép chạy file này độc lập để setup DB
if __name__ == "__main__":
    setup_database_tables()