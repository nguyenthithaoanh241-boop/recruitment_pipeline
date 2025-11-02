# main.py

import sys
import os
import random

# Thêm đường dẫn dự án vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- Import các thành phần của pipeline ---
from pipeline.db_setup import setup_database_tables
from pipeline.transformer import transform_data 

# --- Import các CLASS Scraper ---
from scrapers.TopCV import TopCVScraper
from scrapers.Careerlink import CareerLinkScraper


def run_full_pipeline():
    """
    Chạy toàn bộ pipeline ETL từ đầu đến cuối.
    """
    print("🚀 BẮT ĐẦU CHẠY PIPELINE TUYỂN DỤNG 🚀")
    # Bước 0: Thiết lập database (Giữ nguyên)
    print("\n----- BƯỚC 0: THIẾT LẬP DATABASE -----")
    setup_database_tables()
    
    # Bước 1: Crawl VÀ LOAD dữ liệu từ các nguồn
    print("\n----- BƯỚC 1: CRAWL & LOAD DỮ LIỆU -----")
    try:
        # 1. Khởi tạo các "đối tượng" scraper
        topcv_scraper = TopCVScraper()
        
        careerlink_hardware = CareerLinkScraper(
            category_name="PhanCungMang",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-cung-mang/130"
        )
        
        careerlink_software = CareerLinkScraper(
            category_name="PhanMem",
            base_url="https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"
        )
        
       
        # careerviet_software = CareerVietScraper(
        #     category_name="PhanMem",
        #     base_url="https://careerviet.vn/viec-lam/cntt-phan-mem-c1-vi.html"
        # )
        # careerviet_hardware = CareerVietScraper(
        #     category_name="PhanCung",
        #     base_url="https://careerviet.vn/viec-lam/cntt-phan-cung-mang-c63-vi.html"
        # )

        # 2. Tạo danh sách các đối tượng scraper cần chạy
        scrapers_to_choose_from = [
            topcv_scraper,
            #careerlink_hardware,
            #careerlink_software,
            #careerviet_hardware,
            #careerviet_software
        ]

        # 3. Chọn ngẫu nhiên một đối tượng scraper từ danh sách
        chosen_scraper = random.choice(scrapers_to_choose_from)

        # In ra để biết scraper nào được chọn
        scraper_name = chosen_scraper.__class__.__name__
        category = getattr(chosen_scraper, 'category_name', 'Default') # Lấy category_name nếu có
        print(f"🤖 Lần này sẽ chạy ngẫu nhiên scraper: {scraper_name} (Category: {category})")
        
        # 4. Chạy phương thức .run() của đối tượng đã được chọn
        chosen_scraper.run() 
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình cào dữ liệu (Step 1): {e}")

    # Bước 3: Transform dữ liệu và nạp vào Production
    print("\n----- BƯỚC 3: TRANSFORM DỮ LIỆU SANG PRODUCTION -----")
    # transform_data()
    
    print("\n🎉 PIPELINE HOÀN TẤT! 🎉")

if __name__ == "__main__":
    run_full_pipeline()