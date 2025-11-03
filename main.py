# main.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- Import các thành phần của pipeline ---
from pipeline.transformer import transform_data 
from pipeline.config import DATASET_DIR, DB_TYPE 
from pipeline.db_setup import setup_database_tables 
from pipeline.loader import load_all_csv_to_staging_and_cleanup 

# --- Import các CLASS Scraper ---
from scrapers.TopCV import TopCVScraper
from scrapers.Careerlink import CareerLinkScraper 


def run_scrapers(scrapers: list): 
    """Chạy tất cả các scraper. Không cần thu thập tên file trả về."""
    for scraper in scrapers:
        try:
            scraper_name = scraper.__class__.__name__
            category = getattr(scraper, 'category_name', 'Default')
            print(f"\n🤖 Bắt đầu chạy: {scraper_name} (Category: {category})")
            
            # Hàm .run() vẫn chạy và trả về tên file/None, nhưng ta BỎ QUA kết quả này.
            saved_file = scraper.run() 
            
            if saved_file:
                print(f"-> Đã tạo file: {saved_file}")
            else:
                print(f"-> {scraper_name} không tạo file mới.")
        except Exception as e:
            print(f"❌ Lỗi nghiêm trọng khi chạy {scraper.__class__.__name__}: {e}")
    return 


def run_full_pipeline():
    print("🚀 BẮT ĐẦU CHẠY PIPELINE TUYỂN DỤNG 🚀")
    print("\n----- BƯỚC 0: KIỂM TRA VÀ THIẾT LẬP DATABASE -----")
    setup_database_tables() 
    

    print("\n----- BƯỚC 1: CRAWL DỮ LIỆU THEO THỨ TỰ -----")
    try:
        # 1. Khởi tạo các "đối tượng" scraper
        print("Khởi tạo các scraper...")
        topcv_scraper = TopCVScraper()
        
        careerlink_hardware = CareerLinkScraper(
            category_name="PhanCungMang",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-cung-mang/130"
        )
        
        careerlink_software = CareerLinkScraper(
            category_name="PhanMem",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"
        )
        
        # 2. Chạy TopCV TRƯỚC
        print("\n🤖 Bắt đầu chạy: TopCV")
        try:
            saved_file = topcv_scraper.run()
            if saved_file:
                print(f"-> Đã tạo file: {saved_file}")
            else:
                print("-> TopCV không tạo file mới.")
        except Exception as e:
            print(f"❌ Lỗi khi chạy TopCV: {e}")

        # 3. Chạy CareerLink SAU
        print("\n🤖 Bắt đầu chạy: CareerLink (Phần Cứng)")
        try:
            saved_file = careerlink_hardware.run()
            if saved_file:
                print(f"-> Đã tạo file: {saved_file}")
            else:
                print("-> CareerLink (Phần Cứng) không tạo file mới.")
        except Exception as e:
            print(f"❌ Lỗi khi chạy CareerLink (Phần Cứng): {e}")

        print("\n🤖 Bắt đầu chạy: CareerLink (Phần Mềm)")
        try:
            saved_file = careerlink_software.run()
            if saved_file:
                print(f"-> Đã tạo file: {saved_file}")
            else:
                print("-> CareerLink (Phần Mềm) không tạo file mới.")
        except Exception as e:
            print(f"❌ Lỗi khi chạy CareerLink (Phần Mềm): {e}")
            
        print("\n✅ Hoàn tất chạy TẤT CẢ scraper.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng trong BƯỚC 1: {e}")

    # --- BƯỚC 2: LOAD DỮ LIỆU (SỬ DỤNG HÀM QUÉT TOÀN BỘ) ---
    print("\n----- BƯỚC 2: LOAD TẤT CẢ CSV CHƯA XỬ LÝ VÀO DATABASE -----")

    target_schema = None
    target_table = None
    
    # Xác định Schema và Table Name dựa trên DB_TYPE
    if DB_TYPE == 'sqlserver':
        target_schema = 'dbo'
        target_table = 'Stg_Jobs' 
    elif DB_TYPE == 'postgresql':
        target_schema = 'staging'
        target_table = 'raw_jobs_ta'
    else:
        print(f"❌ LỖI: Không nhận diện được DB_TYPE '{DB_TYPE}' để chọn schema.")
        print("Dừng pipeline.")
        return 

    # GỌI HÀM LOAD MỚI MỘT LẦN DUY NHẤT
    total_loaded = load_all_csv_to_staging_and_cleanup(
        csv_output_dir=DATASET_DIR, # Truyền thư mục đầu ra
        schema=target_schema,
        table_name=target_table 
    )
    
    print(f"\n-> Hoàn tất BƯỚC 2: Đã nạp và dọn dẹp {total_loaded} dòng dữ liệu.")

    # --- BƯỚC 3: TRANSFORM (Giữ nguyên) ---
    #print("\n----- BƯỚC 3: TRANSFORM DỮ LIỆU SANG PRODUCTION -----")
    # transform_data()
    
    print("\n🎉 PIPELINE HOÀN TẤT! 🎉")

if __name__ == "__main__":
    run_full_pipeline()