# main.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- Import các thành phần của pipeline ---
from pipeline.transformer import transform_data 
from pipeline.config import DATASET_DIR, DB_TYPE 

# --- Import các CLASS Scraper ---
from scrapers.TopCV import TopCVScraper
from scrapers.Careerlink import CareerLinkScraper

# --- Import hàm LOADER mới ---
from pipeline.loader import load_csv_to_staging_and_cleanup

# ... (Hàm run_scrapers(scrapers) giữ nguyên, không cần sửa) ...
def run_scrapers(scrapers: list) -> list:
    all_saved_files = []
    for scraper in scrapers:
        try:
            scraper_name = scraper.__class__.__name__
            category = getattr(scraper, 'category_name', 'Default')
            print(f"\n🤖 Bắt đầu chạy: {scraper_name} (Category: {category})")
            saved_file = scraper.run() 
            if saved_file:
                print(f"-> Đã lưu file: {saved_file}")
                all_saved_files.append(saved_file)
            else:
                print(f"-> {scraper_name} không trả về file nào.")
        except Exception as e:
            print(f"❌ Lỗi nghiêm trọng khi chạy {scraper.__class__.__name__}: {e}")
    return all_saved_files


def run_full_pipeline():
    print("🚀 BẮT ĐẦU CHẠY PIPELINE TUYỂN DỤNG 🚀")
    
    # --- BƯỚC 1: CRAWL (Giữ nguyên) ---
    print("\n----- BƯỚC 1: CRAWL DỮ LIỆU (LƯU RA CSV) -----")
    scrapers_to_run = [
        TopCVScraper(),
        CareerLinkScraper(
            category_name="PhanCungMang",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-cung-mang/130"
        ),
        CareerLinkScraper(
            category_name="PhanMem",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"
        ),
    ]
    saved_files = run_scrapers(scrapers_to_run)
    
    if not saved_files:
        print("\nHoàn tất: Không có file nào được cào. Dừng pipeline.")
        return
    print(f"\n-> Hoàn tất BƯỚC 1: {len(saved_files)} file đã được lưu vào {DATASET_DIR}.")

    # --- BƯỚC 2: LOAD DỮ LIỆU (Cập nhật) ---
    print("\n----- BƯỚC 2: LOAD DỮ LIỆU TỪ CSV VÀO DATABASE -----")

    # 👇 TỰ ĐỘNG CHỌN SCHEMA DỰA TRÊN CẤU HÌNH
    target_schema = None
    if DB_TYPE == 'sqlserver':
        target_schema = 'dbo'
    elif DB_TYPE == 'postgresql':
        target_schema = 'public'
    else:
        print(f"❌ LỖI: Không nhận diện được DB_TYPE '{DB_TYPE}' để chọn schema.")
        print("Dừng pipeline.")
        return # Dừng lại nếu không biết nạp vào đâu

    print(f"-> Chế độ: {DB_TYPE}. Dữ liệu sẽ được nạp vào schema: '{target_schema}'")

    for file_name in saved_files:
        full_file_path = os.path.join(DATASET_DIR, file_name)
        
        print("-" * 20)
        load_csv_to_staging_and_cleanup(
            file_path=full_file_path,
            schema=target_schema,       
            table_name='raw_jobs_ta'    
        )
    
    print("\n-> Hoàn tất BƯỚC 2: Dữ liệu đã được nạp và file CSV đã được dọn dẹp.")

    # --- BƯỚC 3: TRANSFORM (Giữ nguyên) ---
    print("\n----- BƯỚC 3: TRANSFORM DỮ LIỆU SANG PRODUCTION -----")
    # transform_data()
    
    print("\n🎉 PIPELINE HOÀN TẤT! 🎉")

if __name__ == "__main__":
    run_full_pipeline()