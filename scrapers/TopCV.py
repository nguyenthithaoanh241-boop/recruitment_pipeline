# scrapers/topcv_scraper.py

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

class TopCVScraper:
    def __init__(self):
        """Khoi tao scraper TopCV."""
        
        self.JOB_LIMIT = 100 # [THEM MOI] Dat gioi han job moi
        
        self.JOBS_PER_BREAK = 50
        self.BREAK_DURATION_MIN = 120
        self.BREAK_DURATION_MAX = 300
        self.BATCH_SIZE_RESTART_DRIVER = 20
        self.SOURCE_WEB = "TopCV"
        
        # Thiet lap duong dan
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)

        self.log_file = os.path.join(scraper_dir, "TopCV.log")
        self.id_history_file = os.path.join(scraper_dir, "TopCV_id_history.txt")
    
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

    # ... (Sau phuong thuc _get_section_details) ...
    def _get_max_page(self, driver):
            """
            [THEM MOI - DA CAP NHAT] Tim trang cuoi cung tu text phan trang.
            """
            try:
                # Tai trang 1 (trang mac dinh) de tim max page
                url = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=newp&page=1&category_family=r257"
                self.logger.info("Dang tai trang 1 de xac dinh so trang toi da (max_page)...")
                driver.get(url)
                
                # Doi cho text phan trang xuat hien (dua theo snippet ban cung cap)
                pagination_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, "job-listing-paginate-text"))
                )
                time.sleep(1) # Cho text render
                
                pagination_text = pagination_element.text.strip() # Vi du: "1 / 82 trang"
                self.logger.debug(f"Tim thay pagination text: '{pagination_text}'")
                
                # Su dung regex de lay so trang lon nhat
                match = re.search(r'/\s*(\d+)\s*trang', pagination_text)
                
                if match:
                    max_page_found = int(match.group(1))
                    self.logger.info(f"Tim thay trang toi đa (max page) la: {max_page_found}")
                    return max_page_found
                else:
                    self.logger.warning(f"Khong the tim thay '.../ X trang' tu text: '{pagination_text}'. Dat max_page = 1.")
                    return 1

            except (TimeoutException, NoSuchElementException):
                self.logger.warning("Khong tim thay element #job-listing-paginate-text. Co the chi co 1 trang. Dat max_page = 1.")
                return 1 # Tra ve 1 neu khong tim thay pagination
            except Exception as e:
                self.logger.error(f"Loi khong xac dinh khi tim max page: {e}. Dat max_page = 1.")
                return 1
    
    def _extract_job_id_from_link(self, link):
        """Trich xuat ID tu link job TopCV."""
        if not link:
            return None
        match = re.search(r'/(\d+)\.html', link)
        return match.group(1) if match else None
        
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
        self.logger.info(f"Bat dau phien cao du lieu TopCV moi (Gioi han {self.JOB_LIMIT} job moi)...")

        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"TopCV_jobs_{now_str}.csv")
        self.logger.info(f"Du lieu se duoc luu vao file: {os.path.basename(output_file)}")

        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            
        driver = self._create_driver()
        existing_ids = self._get_existing_ids(self.id_history_file)
        self.logger.info(f"Da tim thay {len(existing_ids)} ID jobs trong lich su.")

        # ==========================================================
        # [THEM MOI] Logic tim max page
        # ==========================================================
        try:
            # Ham nay se tai trang 1
            max_page_limit = self._get_max_page(driver) 
        except Exception as e:
            self.logger.error(f"Gap loi nghiem trong khi lay max_page: {e}. Dung quet trang.")
            driver.quit()
            return None # Thoat som neu khong lay duoc max_page

        new_jobs_to_crawl = []
        
        # ==========================================================
        # [THAY DOI] Vong 1: Thu thap Link (Tu dong quet)
        # ==========================================================
        page = 1
        jobs_collected_count = 0
        stop_collecting = False
        consecutive_pages_with_no_new_jobs = 0
        MAX_CONSECUTIVE_EMPTY_PAGES = 5 # Van giu logic nay de dung som

        # [THAY DOI] Sua 'while True' thanh 'while page <= max_page_limit'
        while page <= max_page_limit: 
            if stop_collecting:
                self.logger.info(f"Da du {self.JOB_LIMIT} job moi. Dung quet trang.")
                break # Dung vong lap 'while'
            
            if consecutive_pages_with_no_new_jobs >= MAX_CONSECUTIVE_EMPTY_PAGES:
                self.logger.info(f"Dung quet vi {MAX_CONSECUTIVE_EMPTY_PAGES} trang lien tiep khong co job MOI nao (mac du max_page la {max_page_limit}).")
                break # Dung vong lap 'while'

            
            # [DIEU CHINH] Logic tai trang
            if page == 1:
                self.logger.info(f"Dang xu ly trang {page}/{max_page_limit} (da duoc tai de lay max_page)...")
                # Trang 1 da duoc tai boi _get_max_page, khong can driver.get()
            else:
                url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=newp&page={page}&category_family=r257"
                self.logger.info(f"Dang quet trang {page}/{max_page_limit}...")
                try:
                    driver.get(url)
                    WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.job-item-search-result")))
                    time.sleep(random.uniform(2, 4))
                except TimeoutException:
                    self.logger.warning(f"Trang {page} khong ton tai hoac load qua lau. Bo qua trang nay.")
                    page += 1
                    continue # Tiep tuc vong while

            
            job_cards = driver.find_elements(By.CSS_SELECTOR, "div.job-item-search-result")
            if not job_cards:
                self.logger.warning(f"Trang {page} khong co job nao. Chuyen trang tiep theo.")
                page += 1
                consecutive_pages_with_no_new_jobs += 1 # [DIEU CHINH] Van dem trang rong
                continue # Tiep tuc vong while

            new_jobs_found_on_page = 0
            for card in job_cards:
                if stop_collecting:
                    break # Dung vong 'card'

                try:
                    link_element = card.find_element(By.CSS_SELECTOR, "h3.title a")
                    link = link_element.get_attribute("href")
                    job_id = self._extract_job_id_from_link(link)
                    
                    if job_id and (job_id not in existing_ids):
                        new_jobs_to_crawl.append((link, job_id)) 
                        existing_ids.add(job_id)
                        new_jobs_found_on_page += 1
                        jobs_collected_count += 1 # Tang bo dem tong
                        
                        if jobs_collected_count >= self.JOB_LIMIT:
                            stop_collecting = True
                            break # Dung vong 'card'
                except Exception:
                    continue
            
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} -> Tim thay {new_jobs_found_on_page} job MOI. (Tong so job moi: {jobs_collected_count}/{self.JOB_LIMIT})")
                consecutive_pages_with_no_new_jobs = 0 # Reset
            else:
                self.logger.info(f"Trang {page} khong co job nao moi.")
                consecutive_pages_with_no_new_jobs += 1 # Tang
            
            page += 1 # Chuyen sang trang tiep theo
            time.sleep(random.uniform(2, 5)) # Them mot khoang nghi nho giua cac trang

        self.logger.info(f"Hoan thanh quet trang (da quet den trang {page-1} / gioi han {max_page_limit}).")

        self.logger.info(f"Da thu thap xong. Co {len(new_jobs_to_crawl)} job moi can cao chi tiet.")

        success_count, error_count = 0, 0
        if not new_jobs_to_crawl:
            self.logger.info("Khong co job moi nao de cao. Ket thuc.")
        else:
            # ==========================================================
            # Vong 2: Cao chi tiet
            # ==========================================================
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail__body")))
                    time.sleep(random.uniform(2, 5))
                    
                    
                    title, salary, experience, level, recruit_quantity, work_form, education = "", "", "", "", "", "", ""
                    deadline_raw, specialization, work_location, work_time = "", "", "", ""
                    company_name, company_link, company_size = "", "", ""
                    job_description, requirement, benefits = "", "", ""
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
                    
                    # [THEM MOI] Kiem tra gioi han 300 job
                    if success_count >= self.JOB_LIMIT:
                        self.logger.info(f"Da dat gioi han {self.JOB_LIMIT} job thanh cong. Dung cao chi tiet.")
                        break # Dung vong 'for idx, (link, job_id)'

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

        # [XOA] Logic cap nhat max_page da bi xoa
        
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
    print("--- [BAT DAU] Dang chay TopCV Scraper (doc lap) ---")
    try:
        scraper = TopCVScraper()
        scraper.run()
        print("--- [HOAN TAT] TopCV Scraper ---")
    except Exception as e:
        print(f"!!! LOI NGHIEM TRONG (TopCV): {e}")