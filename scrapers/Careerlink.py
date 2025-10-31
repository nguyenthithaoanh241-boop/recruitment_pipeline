# scrapers/careerlink_scraper.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, random, csv, os, datetime, re, sys, logging # <--- THÊM 'sys' và 'logging'

# <--- THÊM MỚI: Import hàm loader từ file script/loader.py
project_root_for_import = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root_for_import)
from pipeline.loader import load_csv_to_staging_and_cleanup

class CareerLinkScraper:
    def __init__(self, category_name, base_url):
        """Khởi tạo scraper cho một danh mục cụ thể trên CareerLink."""
        self.category_name = category_name
        self.base_url = base_url
        self.SOURCE_WEB = "CareerLink"

        # ===== CẤU HÌNH CHUNG =====
        self.PAUSE_BETWEEN_PAGES_MIN = 3
        self.PAUSE_BETWEEN_PAGES_MAX = 6
        self.PAUSE_BETWEEN_JOBS_MIN = 4
        self.PAUSE_BETWEEN_JOBS_MAX = 8
        self.JOBS_PER_LONG_BREAK = 50
        self.LONG_BREAK_DURATION_MIN = 60
        self.LONG_BREAK_DURATION_MAX = 120
        self.JOB_LIMIT = 81
        # ===== THIẾT LẬP ĐƯỜNG DẪN =====
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        # Sử dụng chung file log và history cho toàn bộ CareerLink
        self.log_file = os.path.join(scraper_dir, "CareerLink.log")
        self.id_history_file = os.path.join(scraper_dir, "CareerLink_id_history.txt")

        # <--- SỬA: Thêm 2 cột mới vào Header
        self.CSV_HEADER = [
            "title", "work_location", "salary", "experience", "level", "work_form", "company_name", "company_link",
            "company_size", "gender", "education", "requirement", "job_description", "benefits",
            "post_date", "deadline", "link", "source_web",
            "scraped_at", "transform_status" # <--- 2 cột mới
        ]

        # <--- THÊM MỚI: Thiết lập logger
        self._setup_logging()
        # Tạo 1 logger con riêng cho category này (ví dụ: 'CareerLink.IT-Software')
        self.logger = logging.getLogger(f"{self.SOURCE_WEB}.{self.category_name}") # <--- THÊM MỚI

    def _setup_logging(self): # <--- THÊM MỚI: Hàm thiết lập logging
        """
        Cấu hình base logger 'CareerLink'. 
        Chỉ thêm handler NẾU nó chưa được thiết lập (tránh lặp log khi tạo nhiều instance).
        """
        base_logger = logging.getLogger(self.SOURCE_WEB) # Logger gốc là 'CareerLink'
        base_logger.setLevel(logging.INFO)

        # Chỉ thêm handler nếu logger này chưa có
        if not base_logger.hasHandlers():
            # Định dạng log (bao gồm tên của logger con)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # Handler cho File
            file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            base_logger.addHandler(file_handler)

            # Handler cho Console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            base_logger.addHandler(console_handler)

    def _create_driver(self):
        """Tạo và trả về một instance của Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless=new")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    

    def _get_existing_ids(self):
        """Đọc và trả về một set các ID đã cào từ trước."""
        if not os.path.exists(self.id_history_file): return set()
        try:
            with open(self.id_history_file, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            self.logger.error(f"Lỗi khi đọc file lịch sử ID {self.id_history_file}: {e}") # <--- SỬA
            return set()

    def _extract_job_id_from_link(self, link):
        """Trích xuất ID từ link job của CareerLink."""
        if not link: return None
        match = re.search(r'/(\d+)(?=\?|$)', link)
        return match.group(1) if match else None

    def _human_like_scroll(self, driver):
        """Cuộn trang một cách tự nhiên."""
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        step = random.randint(300, 600)
        while current_position < scroll_height:
            driver.execute_script(f"window.scrollTo(0, {current_position + step});")
            current_position += step
            time.sleep(random.uniform(0.3, 0.8))

    def _safe_text(self, driver, by, selector):
        """Lấy text của element một cách an toàn."""
        try:
            return driver.find_element(by, selector).text.strip()
        except:
            return ""

    def _get_max_page(self, driver, link):
        """Lấy số trang tối đa của một danh mục."""
        driver.get(link)
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.pagination li a")))
            pages = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li a")
            max_page = 1
            for p in pages:
                try: 
                    num = int(p.text.strip())
                    max_page = max(max_page, num)
                except: continue
            return max_page
        except:
            return 1

    def run(self):
        """Phương thức chính để chạy toàn bộ quá trình cào dữ liệu."""
        self.logger.info("🚀 Bắt đầu phiên cào dữ liệu CareerLink mới...") # <--- SỬA
        
        now_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"CareerLink_{self.category_name}_jobs_{now_str}.csv")
        self.logger.info(f"📄 Dữ liệu lần này sẽ được lưu vào file: {os.path.basename(output_file)}") # <--- SỬA
        
        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)

        driver = self._create_driver()
        existing_ids = self._get_existing_ids()
        self.logger.info(f"📊 Đã tìm thấy {len(existing_ids)} ID jobs trong lịch sử chung của CareerLink.") # <--- SỬA
        
        new_jobs_to_crawl = []
        try:
            max_page = self._get_max_page(driver, self.base_url)
            self.logger.info(f"🔎 Link {self.base_url} có tối đa {max_page} trang.") # <--- SỬA
        except Exception as e:
            self.logger.error(f"❌ Không thể lấy số trang tối đa. Lỗi: {e}. Dừng chương trình.") # <--- SỬA
            driver.quit()
            return

        
        for page in range(1, max_page + 1):
            
            url = f"{self.base_url}?page={page}"
            self.logger.info(f"🔎 Đang quét trang {page}: {url}") # <--- SỬA
            try:
                driver.get(url)
                time.sleep(random.uniform(2, 4))
                self._human_like_scroll(driver)
                WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.job-link.clickable-outside")))
            except Exception as e:
                self.logger.warning(f"⚠️ Trang {page} không load được. Bỏ qua trang. Lỗi: {e}") # <--- SỬA
                continue

            job_cards = driver.find_elements(By.CSS_SELECTOR, "a.job-link.clickable-outside")
            if not job_cards:
                self.logger.info(f"✅ Web chỉ có tới trang {page-1}. Dừng thu thập.") # <--- SỬA
                break

            new_jobs_found_on_page = 0
            for card in job_cards:
                try:
                    link_job = card.get_attribute("href")
                    job_id = self._extract_job_id_from_link(link_job)
                    if job_id and job_id not in existing_ids:
                        new_jobs_to_crawl.append((link_job, job_id))
                        existing_ids.add(job_id)
                        new_jobs_found_on_page += 1
                except:
                    continue
            
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} → Tìm thấy {new_jobs_found_on_page} job MỚI.") # <--- SỬA
            else:
                self.logger.info(f"Trang {page} không có job nào mới. (Tiếp tục quét...)") # <--- SỬA

            pause_time = random.uniform(self.PAUSE_BETWEEN_PAGES_MIN, self.PAUSE_BETWEEN_PAGES_MAX)
            self.logger.info(f"--- Nghỉ {round(pause_time, 1)} giây trước khi sang trang tiếp theo ---") # <--- SỬA
            time.sleep(pause_time)

        self.logger.info(f"🎉 Đã thu thập xong. Có {len(new_jobs_to_crawl)} job mới cần cào chi tiết.") # <--- SỬA
        
        success_count, error_count = 0, 0
        
        if not new_jobs_to_crawl:
            self.logger.info("Không có job mới nào để cào. Kết thúc.") # <--- SỬA
        else:
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    time.sleep(random.uniform(2, 5))
                    self._human_like_scroll(driver)
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail")))
                    
                    # <--- THÊM MỚI: Lấy thời gian cào
                    scraped_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    title = self._safe_text(driver, By.CSS_SELECTOR, "h1.job-title.mb-0")
                    work_location = self._safe_text(driver, By.XPATH, "(//div[@class='d-flex align-items-center mb-2'])[1]")
                    salary = self._safe_text(driver, By.XPATH, "(//div[@class='d-flex align-items-center mb-2'])[2]")
                    experience = self._safe_text(driver, By.XPATH, "(//div[@class='d-flex align-items-center mb-2'])[3]")
                    post_date = self._safe_text(driver, By.XPATH, "//div[@id='job-date']//div[contains(@class,'date-from')]//span[last()]")
                    deadline = self._safe_text(driver, By.XPATH, "//div[@id='job-date']//div[contains(@class,'day-expired')]//b")
                    job_description = self._safe_text(driver, By.XPATH, '//div[@id="section-job-description"]//div[@class="rich-text-content"]')
                    skills = self._safe_text(driver, By.XPATH, '//div[@id="section-job-skills"]')
                    benefits = self._safe_text(driver, By.XPATH, '//div[@id="section-job-benefits"]')
                    try:
                        company_elem = driver.find_element(By.CSS_SELECTOR, "h5.company-name-title a")
                        company_name = company_elem.get_attribute("title").strip()
                        company_link = company_elem.get_attribute("href")
                    except: company, company_link = "", ""
                    company_size = self._safe_text(driver, By.XPATH, "//i[contains(@class,'cli-users')]/following-sibling::span")
                    level = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Cấp bậc')]/following-sibling::div")
                    education = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Học vấn')]/following-sibling::div")
                    gender = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Giới tính')]/following-sibling::div")
                    work_form = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Loại công việc')]/following-sibling::div")

                    with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        # <--- SỬA: Thêm 2 cột mới vào dòng
                        writer.writerow([
                            title, work_location, salary, experience, level, work_form,
                            company_name, company_link, company_size, gender, education,
                            skills, job_description, benefits, post_date, deadline, link, self.SOURCE_WEB,
                            scraped_timestamp, 0 # <--- 2 cột mới
                        ])
                    
                    with open(self.id_history_file, "a", encoding="utf-8") as f:
                        f.write(job_id + "\n")

                    success_count += 1
                    self.logger.info(f"✅ [{success_count}/{len(new_jobs_to_crawl)}] Đã cào và lưu job ID {job_id}: {title[:60]}...") # <--- SỬA
                    
                    #mỗi lần cũng chỉ cào đc thêm 81 jobs, lớn hơn là lỗi
                    if success_count >= self.JOB_LIMIT:
                        self.logger.info(f"🔔 Đã đạt giới hạn {self.JOB_LIMIT} job thành công. Dừng cào chi tiết.") # <--- SỬA
                        break # Thoát khỏi vòng lặp cào chi tiết

                    if success_count % self.JOBS_PER_LONG_BREAK == 0 and success_count < len(new_jobs_to_crawl):
                        sleep_time = random.uniform(self.LONG_BREAK_DURATION_MIN, self.LONG_BREAK_DURATION_MAX)
                        self.logger.info(f"⏸ Nghỉ dài sau {success_count} job... Sẽ tiếp tục sau {round(sleep_time/60, 1)} phút.") # <--- SỬA
                        time.sleep(sleep_time)
                    else:
                        time.sleep(random.uniform(self.PAUSE_BETWEEN_JOBS_MIN, self.PAUSE_BETWEEN_JOBS_MAX))
                
                except Exception as e:
                    error_count += 1
                    self.logger.error(f"❌ Lỗi khi xử lý link {idx}/{len(new_jobs_to_crawl)} (ID: {job_id}): {link} | {e}") # <--- SỬA
                    driver.get(self.base_url)
                    time.sleep(5)
            
        driver.quit()
        
        # <--- THÊM MỚI: Logic nạp DB và dọn dẹp file CSV ---
        if success_count > 0:
            self.logger.info(f"--- BẮT ĐẦU NẠP VÀO DATABASE ({os.path.basename(output_file)}) ---")
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
        
        self.logger.info(f"🎉 Crawl xong - Đã lưu {success_count} job MỚI, Lỗi: {error_count}") # <--- SỬA