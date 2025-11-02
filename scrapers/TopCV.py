# scrapers/topcv_scraper.py

import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time, random, csv, os, datetime, sys, re
import logging 

# <--- THÊM MỚI: Import hàm loader từ file script/loader.py
# (Giả sử file script/ nằm cùng cấp với thư mục scrapers/ trong project_root)
project_root_for_import = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root_for_import)
from pipeline.loader import load_csv_to_staging_and_cleanup

class TopCVScraper:
    def __init__(self):
        """Khởi tạo các biến cấu hình và đường dẫn cho scraper TopCV."""
        # ===== CẤU HÌNH CHO PIPELINE =====
        self.START_PAGE = 1
        self.PAGES_TO_ADD_PER_RUN = 1 # Số trang sẽ cộng thêm cho lần chạy kế tiếp
        self.JOBS_PER_BREAK = 50
        self.BREAK_DURATION_MIN = 120
        self.BREAK_DURATION_MAX = 300
        self.BATCH_SIZE_RESTART_DRIVER = 50
        self.SOURCE_WEB = "TopCV"
        
        # ===== THIẾT LẬP ĐƯỜNG DẪN =====
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)

        self.log_file = os.path.join(scraper_dir, "TopCV.log") # <--- SỬA: Đổi .txt thành .log
        self.id_history_file = os.path.join(scraper_dir, "TopCV_id_history.txt")
        self.max_page_file = os.path.join(scraper_dir, "TopCV_max_page.txt")

        # <--- THAY ĐỔI: Thêm 2 cột mới vào Header
        self.CSV_HEADER = [
            "title", "specialization", "work_location", "experience", "salary",
            "work_time", "level", "work_form", "company_name", "company_link",
            "company_size", "recruit_quantity", "education",
            "requirement", "job_description", "benefits", "deadline", "link", "source_web",
            "scraped_at"
        ]
        
        # <--- THÊM MỚI: Thiết lập logger
        self._setup_logging()
        self.logger = logging.getLogger(self.SOURCE_WEB) # <--- Logger riêng cho TopCV

    def _setup_logging(self): # <--- THÊM MỚI: Hàm thiết lập logging
        """Cấu hình logging để ghi ra file và console."""
        logger = logging.getLogger(self.SOURCE_WEB)
        logger.setLevel(logging.INFO) # Chỉ log từ mức INFO trở lên

        # Bỏ các handler cũ nếu đã tồn tại
        if logger.hasHandlers():
            logger.handlers.clear()

        # Định dạng log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Handler cho File
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler cho Console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    def _create_driver(self):
        """Tạo và trả về một instance của Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        #chrome_options.add_argument("--headless=new") # <--- Bỏ comment nếu chạy trên server
        return webdriver.Chrome(options=chrome_options)

    

    def _get_existing_ids(self, file_path):
        """Đọc và trả về một set các ID đã cào từ trước."""
        if not os.path.exists(file_path):
            return set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            self.logger.error(f"Lỗi khi đọc file lịch sử ID {file_path}: {e}") # <--- SỬA: Dùng logger
            return set()

    def _extract_job_id_from_link(self, link):
        """Trích xuất ID từ link job của TopCV."""
        if not link:
            return None
        match = re.search(r'/(\d+)\.html', link)
        return match.group(1) if match else None
        
    def _get_element_text(self, driver, by, value):
        """Helper function để lấy text của element một cách an toàn."""
        try:
            return driver.find_element(by, value).text.strip()
        except NoSuchElementException:
            return ""

    def _get_section_details(self, driver, section_title):
        """Lấy nội dung chi tiết của một mục trong mô tả công việc."""
        try:
            section_elements = driver.find_elements(By.XPATH, f"//h3[contains(text(),'{section_title}')]/following-sibling::div[@class='job-description__item--content']//*")
            texts = [el.text.strip() for el in section_elements if el.text.strip()]
            return ". ".join(texts)
        except NoSuchElementException:
            return ""

    def run(self):
        """Phương thức chính để chạy toàn bộ quá trình cào dữ liệu."""
        start_time = time.time()
        self.logger.info("🚀 Bắt đầu phiên cào dữ liệu TopCV mới...") # <--- SỬA: Dùng logger

        now_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"TopCV_jobs_{now_str}.csv")
        self.logger.info(f"📄 Dữ liệu lần này sẽ được lưu vào file: {os.path.basename(output_file)}") # <--- SỬA

        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            
        try:
            if not os.path.exists(self.max_page_file):
                max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                with open(self.max_page_file, 'w') as f: f.write(str(max_page_to_crawl))
                self.logger.info(f"File max_page.txt không tồn tại. Tạo mới và đặt trang tối đa là {max_page_to_crawl}.") # <--- SỬA
            else:
                with open(self.max_page_file, 'r') as f:
                    content = f.readline().strip()
                    if content and content.isdigit():
                        max_page_to_crawl = int(content)
                    else:
                        max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                        self.logger.warning(f"Nội dung file max_page.txt không hợp lệ. Đặt lại trang tối đa là {max_page_to_crawl}.") # <--- SỬA
        except Exception as e:
            max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
            self.logger.error(f"Lỗi khi đọc file max_page.txt: {e}. Đặt lại trang tối đa là {max_page_to_crawl}.") # <--- SỬA

        self.logger.info(f"📌 Lần này sẽ quét toàn bộ từ trang {self.START_PAGE} → {max_page_to_crawl}.") # <--- SỬA
        
        driver = self._create_driver()
        existing_ids = self._get_existing_ids(self.id_history_file)
        self.logger.info(f"📊 Đã tìm thấy {len(existing_ids)} ID jobs trong lịch sử.") # <--- SỬA

        new_jobs_to_crawl = []
        
        for page in range(self.START_PAGE, max_page_to_crawl + 1):
            url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=newp&type_keyword={page}&category_family=r257"
            
            self.logger.info(f"🔎 Đang quét trang {page}: {url}") # <--- SỬA
            try:
                driver.get(url)
                WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.job-item-search-result")))
                time.sleep(random.uniform(2, 4))
            except TimeoutException:
                self.logger.warning(f"Trang {page} không tồn tại hoặc load quá lâu. Bỏ qua.") # <--- SỬA
                continue
            
            job_cards = driver.find_elements(By.CSS_SELECTOR, "div.job-item-search-result")
            if not job_cards:
                self.logger.warning(f"⚠️ Trang {page} không có job nào. Tiếp tục quét trang tiếp theo.") # <--- SỬA
                continue

            new_jobs_found_on_page = 0
            for card in job_cards:
                try:
                    link_element = card.find_element(By.CSS_SELECTOR, "h3.title a")
                    link = link_element.get_attribute("href")
                    job_id = self._extract_job_id_from_link(link)
                    if job_id and job_id not in existing_ids:
                        new_jobs_to_crawl.append((link, job_id))
                        existing_ids.add(job_id)
                        new_jobs_found_on_page += 1
                except Exception:
                    continue
            
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} → Tìm thấy {new_jobs_found_on_page} job MỚI.") # <--- SỬA
            else:
                self.logger.info(f"Trang {page} không có job nào mới.") # <--- SỬA

        self.logger.info(f"🎉 Đã thu thập xong. Có {len(new_jobs_to_crawl)} job mới cần cào chi tiết.") # <--- SỬA

        success_count, error_count = 0, 0
        if not new_jobs_to_crawl:
            self.logger.info("Không có job mới nào để cào. Kết thúc.") # <--- SỬA
        else:
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail__body")))
                    time.sleep(random.uniform(2, 5))
                    
                    # <--- THÊM MỚI: Lấy thời gian cào ngay tại thời điểm này
                    scraped_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    title = self._get_element_text(driver, By.CSS_SELECTOR, "h1.job-detail__info--title")
                    salary = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Mức lương')]]/div[contains(@class, 'value')]")
                    experience = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Kinh nghiệm')]]/div[contains(@class, 'value')]")
                    level = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Cấp bậc')]]/div[contains(@class, 'value')]")
                    recruit_quantity = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Số lượng tuyển')]]/div[contains(@class, 'value')]")
                    work_form = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Hình thức làm việc')]]/div[contains(@class, 'value')]")
                    education = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Học vấn')]]/div[contains(@class, 'value')]")
                    deadline_raw = self._get_element_text(driver, By.CSS_SELECTOR, "div.job-detail__info--deadline")

                    specialization = self._get_element_text(driver, By.CSS_SELECTOR, "a.item.search-from-tag.link")
                    work_location = self._get_element_text(driver, By.XPATH, "//h3[contains(text(),'Địa điểm làm việc')]/following-sibling::div")
                    work_time = self._get_element_text(driver, By.XPATH,"//h3[contains(text(),'Thời gian làm việc')]/following-sibling::div")
                    
                    company_name = self._get_element_text(driver, By.CSS_SELECTOR, "a.name")
                    company_link = driver.find_element(By.CSS_SELECTOR, ".job-detail__box--right.job-detail__company a").get_attribute("href") if company_name else ""
                    company_size = self._get_element_text(driver, By.XPATH, "//div[contains(@class, 'company-scale')]//div[@class='company-value']")
                    
                    job_description = self._get_section_details(driver, "Mô tả công việc")
                    requirement = self._get_section_details(driver, "Yêu cầu ứng viên")
                    benefits = self._get_section_details(driver, "Quyền lợi")

                    # <--- SỬA: Thêm 2 cột mới vào dữ liệu
                    job_data = [
                        title, specialization, work_location, experience, salary, work_time, level, work_form,
                        company_name, company_link, company_size, recruit_quantity, education, requirement, job_description, benefits,
                        deadline_raw.replace('Hạn nộp hồ sơ: ', ''), link, self.SOURCE_WEB,
                        scraped_timestamp # <--- 2 cột mới (thời gian cào, trạng thái transform = 0)
                    ]
                    
                    with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(job_data)

                    with open(self.id_history_file, "a", encoding="utf-8") as f:
                        f.write(job_id + "\n")
                    
                    success_count += 1
                    self.logger.info(f"✅ [{success_count}/{len(new_jobs_to_crawl)}] Đã cào và lưu job ID {job_id}: {title}") # <--- SỬA
                    
                    if success_count % self.JOBS_PER_BREAK == 0 and success_count < len(new_jobs_to_crawl):
                        pause_time = random.uniform(self.BREAK_DURATION_MIN, self.BREAK_DURATION_MAX)
                        self.logger.info(f"⏸ Đã cào {success_count} job. Tạm nghỉ {round(pause_time/60, 1)} phút...") # <--- SỬA
                        time.sleep(pause_time)
                    
                    if idx % self.BATCH_SIZE_RESTART_DRIVER == 0 and idx < len(new_jobs_to_crawl):
                        self.logger.info("🔄 Khởi động lại trình duyệt...") # <--- SỬA
                        driver.quit()
                        time.sleep(random.uniform(20, 40))
                        driver = self._create_driver()

                except Exception as e:
                    error_count += 1
                    self.logger.error(f"❌ Lỗi khi xử lý link {idx}/{len(new_jobs_to_crawl)} (ID: {job_id}): {link} | {e}") # <--- SỬA
            
        driver.quit()

        # <--- THÊM MỚI: Logic nạp DB và dọn dẹp file CSV ---
        if success_count > 0:
            self.logger.info(f"--- BẮT ĐẦU NẠP VÀO DATABASE ---")
            load_csv_to_staging_and_cleanup(output_file, schema='staging', table_name='raw_jobs')
            self.logger.info(f"--- KẾT THÚC NẠP VÀO DATABASE ---")
        elif not new_jobs_to_crawl:
            self.logger.info("Không có job mới, không cần nạp vào DB.")
            try:
                os.remove(output_file) # Xóa file CSV rỗng (chỉ có header)
                self.logger.info(f"Đã xóa file CSV rỗng: {output_file}")
            except Exception as e:
                self.logger.error(f"Không thể xóa file rỗng {output_file}: {e}")
        else: # Có job mới nhưng cào lỗi 100%
            self.logger.warning(f"Tất cả {len(new_jobs_to_crawl)} job mới đều cào bị lỗi. Không nạp vào DB.")
            try:
                os.remove(output_file) # Xóa file CSV rỗng (chỉ có header)
                self.logger.info(f"Đã xóa file CSV rỗng: {output_file}")
            except Exception as e:
                self.logger.error(f"Không thể xóa file rỗng {output_file}: {e}")
        # --- Hết khối code thêm mới ---

        new_max_page = max_page_to_crawl + self.PAGES_TO_ADD_PER_RUN
        try:
            with open(self.max_page_file, "w") as f: f.write(str(new_max_page))
            self.logger.info(f"🔄 Đã cập nhật max_page.txt cho lần chạy tiếp theo: {new_max_page}") # <--- SỬA
        except Exception as e:
            self.logger.error(f"❌ Không thể cập nhật file max_page.txt: {e}") # <--- SỬA

        end_time = time.time()
        total_minutes = round((end_time - start_time) / 60, 2)
        self.logger.info(f"🏁 Crawl xong trong {total_minutes} phút - Đã lưu {success_count} job MỚI, Lỗi: {error_count}") # <--- SỬA