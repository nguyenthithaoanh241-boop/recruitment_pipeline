# scrapers/JobsGo_scraper.py

import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time, random, csv, os, sys, re
from datetime import datetime 
import logging 

# Them project root vao sys.path de chay doc lap
project_root_for_import = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root_for_import)

class JobsGoScraper:
    def __init__(self):
        """Khoi tao scraper JobsGo."""
        # Cau hinh pipeline
        self.START_PAGE = 1
        self.PAGES_TO_ADD_PER_RUN = 1 
        self.JOBS_PER_BREAK = 50
        self.BREAK_DURATION_MIN = 120
        self.BREAK_DURATION_MAX = 300
        self.BATCH_SIZE_RESTART_DRIVER = 20
        self.SOURCE_WEB = "JobsGo"
        
        # Thiet lap duong dan
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)

        self.log_file = os.path.join(scraper_dir, "JobsGo.log")
        self.id_history_file = os.path.join(scraper_dir, "JobsGo_id_history.txt")
        self.max_page_file = os.path.join(scraper_dir, "JobsGo_max_page.txt")

        # ==========================================================
        # SUA 1: Header CSV (Them GioiTinh, LinhVuc cho du 22 cot)
        # ==========================================================
        self.CSV_HEADER = [
            "CongViec", "ChuyenMon", "ViTri", "YeuCauKinhNghiem", "MucLuong",
            "ThoiGianLamViec", "GioiTinh", "CapBac", "HinhThucLamViec", "CongTy", "LinkCongTy",
            "QuyMoCongTy", "SoLuongTuyen", "HocVan",
            "YeuCauUngVien", "MoTaCongViec", "QuyenLoi", "HanNopHoSo", "LinkBaiTuyenDung", "Nguon","NgayCaoDuLieu",
            "LinhVuc"
        ]
        
        self._setup_logging()
        self.logger = logging.getLogger(self.SOURCE_WEB)

    def _setup_logging(self): 
        """Cau hinh logging."""
        logger = logging.getLogger(self.SOURCE_WEB)
        logger.setLevel(logging.INFO)

        if logger.hasHandlers():
            logger.handlers.clear()

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    def _create_driver(self):
        """Tao Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless=new")
        return webdriver.Chrome(options=chrome_options)

    def _get_existing_ids(self, file_path):
        """Doc va tra ve set ID da cao tu file history."""
        if not os.path.exists(file_path):
            return set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            self.logger.error(f"Loi khi doc file lich su ID {file_path}: {e}")
            return set()

    
        
    def _get_element_text(self, driver, by, value):
        """Lay text cua element an toan."""
        try:
            return driver.find_element(by, value).text.strip()
        except NoSuchElementException:
            return ""

    def _get_section_details(self, driver, section_title):
        """Lay noi dung chi tiet cua mot muc (Mo ta, Yeu cau, Quyen loi)."""
        try:
            section_elements = driver.find_elements(By.XPATH, f"//h3[contains(text(),'{section_title}')]/following-sibling::div[@class='job-description__item--content']//*")
            texts = [el.text.strip() for el in section_elements if el.text.strip()]
            return ". ".join(texts)
        except NoSuchElementException:
            return ""

    def run(self):
        """Phuong thuc chinh de chay toan bo qua trinh cao du lieu."""
        start_time = time.time()
        self.logger.info("Bat dau phien cao du lieu JobsGo moi...")

        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"JobsGo_jobs_{now_str}.csv")
        self.logger.info(f"Du lieu se duoc luu vao file: {os.path.basename(output_file)}")

        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            
        try:
            if not os.path.exists(self.max_page_file):
                max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                with open(self.max_page_file, 'w') as f: f.write(str(max_page_to_crawl))
                self.logger.info(f"File max_page.txt khong ton tai. Tao moi va dat trang toi da la {max_page_to_crawl}.")
            else:
                with open(self.max_page_file, 'r') as f:
                    content = f.readline().strip()
                    if content and content.isdigit():
                        max_page_to_crawl = int(content)
                    else:
                        max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                        self.logger.warning(f"Noi dung file max_page.txt khong hop le. Dat lai trang toi da la {max_page_to_crawl}.")
        except Exception as e:
            max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
            self.logger.error(f"Loi khi doc file max_page.txt: {e}. Dat lai trang toi da la {max_page_to_crawl}.")

        self.logger.info(f"Lan nay se quet tu trang {self.START_PAGE} -> {max_page_to_crawl}.")
        
        driver = self._create_driver()
        existing_ids = self._get_existing_ids(self.id_history_file)
        self.logger.info(f"Da tim thay {len(existing_ids)} ID jobs trong lich su.")

        new_jobs_to_crawl = []
        
        
        
        
        for page in range(self.START_PAGE, max_page_to_crawl + 1):
            url = f"https://jobsgo.vn/viec-lam-cong-nghe-thong-tin.html?category=cong-nghe-thong-tin&sort=created&page={page}"
            
            self.logger.info(f"Dang quet trang {page}: {url}")
            try:
                driver.get(url)
                WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.card job-card rounded-3 h-100 p-3")))
                time.sleep(random.uniform(2, 4))
            except TimeoutException:
                self.logger.warning(f"Trang {page} khong ton tai hoac load qua lau. Bo qua.")
                continue
            
            job_cards = driver.find_elements(By.CSS_SELECTOR, "div.card job-card rounded-3 h-100 p-3")
            if not job_cards:
                self.logger.warning(f"Trang {page} khong co job nao. Tiep tuc quet trang tiep theo.")
                continue

            new_jobs_found_on_page = 0
            for card in job_cards:
                try:
                    link_element = card.find_element(By.CSS_SELECTOR, "a")
                    link = link_element.get_attribute("href")
                    
                    
                    
                    if job_id :
                        new_jobs_to_crawl.append((link, job_id)) 
                        new_jobs_found_on_page += 1
                except Exception:
                    continue
            
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} -> Tim thay {new_jobs_found_on_page} job MOI.")
            else:
                self.logger.info(f"Trang {page} khong co job nao moi.")

        self.logger.info(f"Da thu thap xong. Co {len(new_jobs_to_crawl)} job moi can cao chi tiet.")

        success_count, error_count = 0, 0
        if not new_jobs_to_crawl:
            self.logger.info("Khong co job moi nao de cao. Ket thuc.")
        else:
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail__body")))
                    time.sleep(random.uniform(2, 5))
                    
                    
                    title, salary, experience, level, recruit_quantity, work_form, education = "", "", "", "", "", "", ""
                    deadline_raw, specialization, work_location, work_time = "", "", "", ""
                    company_name, company_link, company_size = "", "", ""
                    job_description, requirement, benefits = "", "", ""
                    # Them 2 cot moi de khop DB (JobsGo khong co 2 cot nay)
                    gioi_tinh = "" 
                    linh_vuc = ""

                    # --- Bat dau cao chi tiet (Da khoi phuc dau tieng Viet) ---
                    try:
                        title = self._get_element_text(driver, By.CSS_SELECTOR, "h1.job-detail__info--title")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'title' (ID: {job_id}): {e}")

                    try:
                        salary = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Mức lương')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'salary' (ID: {job_id}): {e}")

                    try:
                        experience = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Kinh nghiệm')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'experience' (ID: {job_id}): {e}")
                    
                    try:
                        level = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Cấp bậc')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'level' (ID: {job_id}): {e}")

                    try:
                        recruit_quantity = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Số lượng tuyển')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'recruit_quantity' (ID: {job_id}): {e}")

                    try:
                        work_form = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Hình thức làm việc')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'work_form' (ID: {job_id}): {e}")

                    try:
                        education = self._get_element_text(driver, By.XPATH, "//div[div[contains(text(), 'Học vấn')]]/div[contains(@class, 'value')]")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'education' (ID: {job_id}): {e}")

                    try:
                        deadline_raw = self._get_element_text(driver, By.CSS_SELECTOR, "div.job-detail__info--deadline")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'deadline_raw' (ID: {job_id}): {e}")

                    try:
                        specialization = self._get_element_text(driver, By.CSS_SELECTOR, "a.item.search-from-tag.link")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'specialization' (ID: {job_id}): {e}")

                    try:
                        work_location = self._get_element_text(driver, By.XPATH, "//div[contains(text(), 'Địa điểm') and contains(@class, 'job-detail__info--section-content-title')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'work_location' (ID: {job_id}): {e}")

                    try:
                        work_time = self._get_element_text(driver, By.XPATH,"//h3[contains(text(),'Thời gian làm việc')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'work_time' (ID: {job_id}): {e}")
                    
                    try:
                        company_name = self._get_element_text(driver, By.CSS_SELECTOR, "a.name")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'company_name' (ID: {job_id}): {e}")

                    try:
                        if company_name: # Chi thu tim link neu co ten cong ty
                            company_link = driver.find_element(By.CSS_SELECTOR, ".job-detail__box--right.job-detail__company a").get_attribute("href")
                    except Exception as e: 
                        self.logger.warning(f"Loi nho khi cao 'company_link' (ID: {job_id}): {e}")
                        company_link = "" 

                    try:
                        company_size = self._get_element_text(driver, By.XPATH, "//div[contains(@class, 'company-scale')]//div[@class='company-value']")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'company_size' (ID: {job_id}): {e}")
                    
                    try:
                        job_description = self._get_section_details(driver, "Mô tả công việc")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'job_description' (ID: {job_id}): {e}")

                    try:
                        requirement = self._get_section_details(driver, "Yêu cầu ứng viên")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'requirement' (ID: {job_id}): {e}")

                    try:
                        benefits = self._get_section_details(driver, "Quyền lợi")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'benefits' (ID: {job_id}): {e}")
                    
                    ngay_cao_hien_tai = datetime.now().strftime('%Y-%m-%d')
                    
                    
                    # ==========================================================
                    # Ghi du 22 cot vao CSV theo dung thu tu
                    # ==========================================================
                    job_data = [
                        title, specialization, work_location, experience, salary,
                        work_time, gioi_tinh, level, work_form,
                        company_name, company_link, company_size, recruit_quantity, education,
                        requirement, job_description, benefits,
                        deadline_raw.replace('Hạn nộp hồ sơ: ', ''), link, self.SOURCE_WEB, ngay_cao_hien_tai,
                        linh_vuc
                    ]
                    
                    # Ghi vao CSV
                    with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(job_data)

                    # Ghi vao lich su ID
                    with open(self.id_history_file, "a", encoding="utf-8") as f:
                        f.write(job_id + "\n")
                    
                    success_count += 1
                    self.logger.info(f"[{success_count}/{len(new_jobs_to_crawl)}] Da cao va luu job ID {job_id}: {title}")
                    
                    # Tam nghi
                    if success_count % self.JOBS_PER_BREAK == 0 and idx < len(new_jobs_to_crawl):
                        sleep_time = random.uniform(self.BREAK_DURATION_MIN, self.BREAK_DURATION_MAX)
                        self.logger.info(f"--- Tam nghi {sleep_time/60:.2f} phut ---")
                        time.sleep(sleep_time)

                    # Khoi dong lai driver
                    if success_count % self.BATCH_SIZE_RESTART_DRIVER == 0 and idx < len(new_jobs_to_crawl):
                        self.logger.info("--- Khoi dong lai driver de giai phong bo nho ---")
                        driver.quit()
                        time.sleep(5)
                        driver = self._create_driver()

                except Exception as e:
                    error_count += 1
                    self.logger.error(f"Loi NGHIEM TRONG khi xu ly link {idx}/{len(new_jobs_to_crawl)} (ID: {job_id}): {link} | {e}")
            
        driver.quit()

        # 1. Cap nhat max_page
        new_max_page = max_page_to_crawl + self.PAGES_TO_ADD_PER_RUN
        try:
            with open(self.max_page_file, "w") as f: f.write(str(new_max_page))
            self.logger.info(f"Da cap nhat max_page.txt cho lan chay tiep theo: {new_max_page}")
        except Exception as e:
            self.logger.error(f"Khong the cap nhat file max_page.txt: {e}")

        # 2. Tinh thoi gian chay
        end_time = time.time()
        total_minutes = round((end_time - start_time) / 60, 2)
        self.logger.info(f"Crawl xong trong {total_minutes} phut - Da luu {success_count} job MOI, Loi: {error_count}")

        # 3. Tra ve ket qua (Xoa file neu rong)
        if success_count > 0:
            return os.path.basename(output_file)
        
        if os.path.exists(output_file):
            try:
                os.remove(output_file) 
                self.logger.info(f"Da xoa file CSV rong/loi: {os.path.basename(output_file)}")
            except Exception as e:
                self.logger.error(f"Khong the xoa file CSV rong {os.path.basename(output_file)}: {e}")
        
        return None

if __name__ == '__main__':
    # Thiet lap sys.path da co o dau file
    print("--- [BAT DAU] Dang chay JobsGo Scraper (doc lap) ---")
    try:
        scraper = JobsGoScraper()
        scraper.run()
        print("--- [HOAN TAT] JobsGo Scraper ---")
    except Exception as e:
        print(f"!!! LOI NGHIEM TRONG (JobsGo): {e}")