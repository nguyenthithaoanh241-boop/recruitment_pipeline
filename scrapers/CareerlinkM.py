import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time, random, csv, os, re, sys, logging
from datetime import datetime

# Them project root vao sys.path de chay doc lap
project_root_for_import = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root_for_import)


class CareerLinkScraper:
    def __init__(self, category_name, base_url):
        """Khoi tao scraper cho mot danh muc CareerLink."""
        self.category_name = category_name
        self.base_url = base_url
        self.SOURCE_WEB = "CareerLink"

        # Cau hinh chung
        self.PAUSE_BETWEEN_PAGES_MIN = 3
        self.PAUSE_BETWEEN_PAGES_MAX = 6
        self.PAUSE_BETWEEN_JOBS_MIN = 4
        self.PAUSE_BETWEEN_JOBS_MAX = 8
        self.JOBS_PER_LONG_BREAK = 50
        self.LONG_BREAK_DURATION_MIN = 60
        self.LONG_BREAK_DURATION_MAX = 120
        self.JOB_LIMIT = 20
        
        # Thiet lap duong dan
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        # Su dung chung file log/history cho CareerLink
        self.log_file = os.path.join(scraper_dir, "CareerLink.log")
        self.id_history_file = os.path.join(scraper_dir, "CareerLink_id_history.txt")

        
        # ==========================================================
        # SUA 1: Header CSV (Cap nhat du 22 cot)
        # ==========================================================
        self.CSV_HEADER = [
            "CongViec","ViTri", "YeuCauKinhNghiem", "MucLuong",
            "ThoiGianLamViec", "GioiTinh", "CapBac", "HinhThucLamViec", "CongTy", "LinkCongTy",
            "QuyMoCongTy", "SoLuongTuyen", "HocVan",
            "YeuCauUngVien", "MoTaCongViec", "QuyenLoi", "HanNopHoSo", "LinkBaiTuyenDung", "Nguon","NgayCaoDuLieu",
            "LinhVuc"
        ]
        
        self._setup_logging()
        self.logger = logging.getLogger(f"{self.SOURCE_WEB}.{self.category_name}") 

    def _setup_logging(self): 
        
        base_logger = logging.getLogger(self.SOURCE_WEB)
        base_logger.setLevel(logging.INFO)

        if not base_logger.hasHandlers():
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            base_logger.addHandler(file_handler)

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            base_logger.addHandler(console_handler)

    def _create_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless=new")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    
    def _get_existing_ids(self):
        """Doc va tra ve set ID da cao tu file history."""
        if not os.path.exists(self.id_history_file): return set()
        try:
            with open(self.id_history_file, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            self.logger.error(f"Loi khi doc file lich su ID {self.id_history_file}: {e}") 
            return set()

    def _extract_job_id_from_link(self, link):
        """Trich xuat ID tu link job CareerLink."""
        if not link: return None
        match = re.search(r'/(\d+)(?=\?|$)', link) 
        return match.group(1) if match else None

    def _human_like_scroll(self, driver):
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        step = random.randint(300, 600)
        while current_position < scroll_height:
            driver.execute_script(f"window.scrollTo(0, {current_position + step});")
            current_position += step
            time.sleep(random.uniform(0.3, 0.8))

    def _safe_text(self, driver, by, selector):
        """Lay text an toan, tra ve "" neu loi."""
        try:
            return driver.find_element(by, selector).text.strip()
        except:
            return ""

    # ==================================================
    # [THEM MOI] HAM TIM MAX PAGE
    # ==================================================
    def _get_max_page(self, driver):
        """
        [THEM MOI] Tim trang cuoi cung tu thanh phan trang (pagination) tren trang 1.
        """
        try:
            # Tai trang 1 (la self.base_url)
            self.logger.info(f"Dang tai trang 1 ({self.base_url}) de xac dinh so trang toi da (max_page)...")
            driver.get(self.base_url) # base_url la trang 1
            
            # Doi cho thanh pagination xuat hien
            pagination_ul = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.pagination"))
            )
            self.logger.debug("Da tim thay thanh pagination.")
            time.sleep(1) # Cho render

            # Tim tat ca cac link co so trang
            page_links = pagination_ul.find_elements(By.XPATH, ".//a[contains(@href, 'page=')]")
            
            if not page_links:
                self.logger.warning("Tim thay pagination nhung khong co link trang. Dat max_page = 1.")
                return 1

            max_page_found = 1
            for link in page_links:
                href = link.get_attribute('href')
                if not href:
                    continue
                
                # Tim so trang tu param ?page=X
                match = re.search(r'page=(\d+)', href)
                if match:
                    try:
                        page_num = int(match.group(1))
                        if page_num > max_page_found:
                            max_page_found = page_num
                    except ValueError:
                        continue
                        
            self.logger.info(f"Tim thay trang toi da (max page) la: {max_page_found}")
            return max_page_found

        except (TimeoutException, NoSuchElementException):
            self.logger.warning("Khong tim thay thanh phan trang. Co the chi co 1 trang. Dat max_page = 1.")
            return 1 # Tra ve 1 neu khong tim thay pagination
        except Exception as e:
            self.logger.error(f"Loi khong xac dinh khi tim max page: {e}. Dat max_page = 1.")
            return 1
            
    def run(self):
        
        start_time = time.time()
        self.logger.info(f"Bat dau phien cao du lieu [{self.category_name}] (Gioi han {self.JOB_LIMIT} job moi)...") 
        
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"CareerLink_{self.category_name}_jobs_{now_str}.csv")
        self.logger.info(f"Du lieu lan nay se duoc luu vao file: {os.path.basename(output_file)}") 
        
        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)

        driver = self._create_driver()
        existing_ids = self._get_existing_ids()
        self.logger.info(f"Da tim thay {len(existing_ids)} ID jobs trong lich su chung cua CareerLink.") 
        
        # ==========================================================
        # [THEM MOI] Logic tim max page
        # ==========================================================
        try:
            # Ham nay se tai trang 1
            max_page_limit = self._get_max_page(driver) 
        except Exception as e:
            self.logger.error(f"Gap loi nghiem trong khi lay max_page: {e}. Dung quet trang.")
            driver.quit()
            return None # Thoat som
        
        new_jobs_to_crawl = []
        
        
        page = 1
        jobs_collected_count = 0
        stop_collecting = False
        consecutive_pages_with_no_new_jobs = 0
        MAX_CONSECUTIVE_EMPTY_PAGES = 5 # Dung neu 5 trang lien tiep khong co job moi

        # [THAY DOI] Sua 'while True' thanh 'while page <= max_page_limit'
        while page <= max_page_limit:
            if stop_collecting:
                self.logger.info(f"Da du {self.JOB_LIMIT} job moi. Dung quet trang.")
                break # Dung vong lap 'while'
            
            if consecutive_pages_with_no_new_jobs >= MAX_CONSECUTIVE_EMPTY_PAGES:
                self.logger.info(f"Dung quet vi {MAX_CONSECUTIVE_EMPTY_PAGES} trang lien tiep khong co job MOI nao (mac du max_page la {max_page_limit}).")
                break # Dung vong lap 'while'
            
            # [DIEU CHINH] Logic tai trang
            try:
                if page == 1:
                    self.logger.info(f"Dang xu ly trang {page}/{max_page_limit} (da duoc tai de lay max_page)...")
                    # Trang 1 da duoc tai, chi can scroll/wait
                    self._human_like_scroll(driver)
                    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.job-link.clickable-outside")))
                else:
                    url = f"{self.base_url}?page={page}"
                    self.logger.info(f"Dang quet trang {page}/{max_page_limit}...") 
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    self._human_like_scroll(driver)
                    WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.job-link.clickable-outside")))
            except Exception as e:
                self.logger.warning(f"Trang {page} khong load duoc. Bo qua trang. Loi: {e}") 
                page += 1
                continue

            job_cards = driver.find_elements(By.CSS_SELECTOR, "a.job-link.clickable-outside")
            
            # [THAY DOI] Logic xu ly khi khong co job card
            if not job_cards:
                self.logger.info(f"Trang {page} khong co job nao. Chuyen trang tiep theo.") 
                consecutive_pages_with_no_new_jobs += 1 # Dem trang rong
                page += 1 # Tang trang
                continue # Tiep tuc vong while, thay vi 'break'

            new_jobs_found_on_page = 0
            for card in job_cards:
                # [THEM MOI] Kiem tra gioi han trong vong lap
                if jobs_collected_count >= self.JOB_LIMIT:
                    stop_collecting = True
                    break # Dung vong 'card'

                try:
                    link_job = card.get_attribute("href")
                    job_id = self._extract_job_id_from_link(link_job)
                    if job_id and job_id not in existing_ids:
                        new_jobs_to_crawl.append((link_job, job_id))
                        existing_ids.add(job_id) # Them vao set de khong bi trung trong phien nay
                        new_jobs_found_on_page += 1
                        jobs_collected_count += 1 # Tang bo dem tong
                except:
                    continue
            
            # [THAY DOI] Logic kiem tra trang rong
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} -> Tim thay {new_jobs_found_on_page} job MOI. (Tong so job moi: {jobs_collected_count}/{self.JOB_LIMIT})")
                consecutive_pages_with_no_new_jobs = 0 # Reset
            else:
                self.logger.info(f"Trang {page} khong co job nao moi.") 
                consecutive_pages_with_no_new_jobs += 1 # Tang

            pause_time = random.uniform(self.PAUSE_BETWEEN_PAGES_MIN, self.PAUSE_BETWEEN_PAGES_MAX)
            self.logger.info(f"--- Nghi {round(pause_time, 1)} giay truoc khi sang trang tiep theo ---") 
            time.sleep(pause_time)
            
            page += 1 # Tang trang de quet tiep
            
        # ==========================================================
        # HET VONG 1
        # ==========================================================
        
        self.logger.info(f"Hoan thanh quet trang (da quet den trang {page-1} / gioi han {max_page_limit}).")

        self.logger.info(f"Da thu thap xong. Co {len(new_jobs_to_crawl)} job moi can cao chi tiet.") 
        
        success_count, error_count = 0, 0
        
        if not new_jobs_to_crawl:
            self.logger.info("Khong co job moi nao de cao. Ket thuc.") 
        else:
            # Vong 2: Cao chi tiet
        # (LOGIC VONG 2 GIU NGUYEN - KHONG THAY DOI)
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    time.sleep(random.uniform(2, 5))
                    self._human_like_scroll(driver)
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail")))
                    
                    
                    # ==========================================================
                    # SUA 2: Khoi tao du 22 bien (them cac cot thieu)
                    # ==========================================================
                    title, work_location, salary, experience, deadline = "", "", "", "", ""
                    job_description, skills, benefits = "", "", ""
                    company_name, company_link, company_size = "", "", ""
                    level, education, gender, work_form = "", "", "", ""
                    
                    # Them cac cot thieu de khop DB
                    
                    work_time = ""
                    recruit_quantity = "" 
                    linh_vuc = "" # Khoi tao bien linh_vuc
                    
                    # Cao tung bien
                    try:
                        title = self._safe_text(driver, By.CSS_SELECTOR, "h1.job-title.mb-0")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'title' (ID: {job_id}): {e}")
                    
                    try:
                        work_location = self._safe_text(driver, By.XPATH, '//div[@id="job-location"]//a')
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'work_location' (ID: {job_id}): {e}")

                    try:
                        salary = self._safe_text(driver, By.XPATH, '//div[@id="job-salary"]/span[contains(@class, "text-primary")]')
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'salary' (ID: {job_id}): {e}")

                    try:
                        experience = self._safe_text(driver, By.XPATH, '//div[i[contains(@class, "cli-suitcase-simple")]]/span')
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'experience' (ID: {job_id}): {e}")

                    try:
                        deadline = self._safe_text(driver, By.XPATH, "//div[@id='job-date']//div[contains(@class,'day-expired')]//b")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'deadline' (ID: {job_id}): {e}")

                    try:
                        job_description_elem = driver.find_element(By.XPATH, '//div[@id="section-job-description"]//div[@class="rich-text-content"]')
                        job_description = job_description_elem.text.strip()
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'job_description' (ID: {job_id}): {e}")

                    try:
                        skills = self._safe_text(driver, By.XPATH, '//div[@id="section-job-skills"]')
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'skills' (ID: {job_id}): {e}")

                    try:
                        benefits_elem = driver.find_element(By.XPATH, '//div[@id="section-job-benefits"]')
                        benefits = benefits_elem.text.strip()
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'benefits' (ID: {job_id}): {e}")
                    
                    try:
                        company_elem = driver.find_element(By.CSS_SELECTOR, "h5.company-name-title a")
                        company_name = company_elem.get_attribute("title").strip()
                        company_link = company_elem.get_attribute("href")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'company_name/link' (ID: {job_id}): {e}")
                    
                    try:
                        company_size = self._safe_text(driver, By.XPATH, "//i[contains(@class,'cli-users')]/following-sibling::span")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'company_size' (ID: {job_id}): {e}")

                    try:
                        level = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Cấp bậc')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'level' (ID: {job_id}): {e}")

                    try:
                        education = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Học vấn')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'education' (ID: {job_id}): {e}")

                    try:
                        gender = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Giới tính')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'gender' (ID: {job_id}): {e}")
                    try:
                        linh_vuc = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Ngành nghề')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'linh_vuc' (ID: {job_id}): {e}")

                    try:
                        work_form = self._safe_text(driver, By.XPATH, "//div[contains(text(),'Loại công việc')]/following-sibling::div")
                    except Exception as e:
                        self.logger.warning(f"Loi nho khi cao 'work_form' (ID: {job_id}): {e}")
                    
                    
                    ngay_cao_hien_tai = datetime.now().strftime('%Y-%m-%d')
                    
                    # Ghi vao CSV
                    with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        
                        # ==========================================================
                        # SUA 3: Ghi du 22 cot theo dung thu tu
                        # ==========================================================
                        writer.writerow([
                            title, work_location, experience, salary,
                            work_time, gender, level, work_form,
                            company_name, company_link, company_size, recruit_quantity, education,
                            skills,
                            job_description, benefits,
                            deadline, link, self.SOURCE_WEB, ngay_cao_hien_tai,
                            linh_vuc
                        ])
                    
                    # Ghi vao history ID
                    with open(self.id_history_file, "a", encoding="utf-8") as f:
                        f.write(job_id + "\n")

                    success_count += 1
                    self.logger.info(f"[{success_count}/{len(new_jobs_to_crawl)}] Da cao va luu job ID {job_id}: {title[:60]}...") 
                    
                    # [GIU NGUYEN] Kiem tra gioi han o Vong 2 van rat tot
                    if success_count >= self.JOB_LIMIT:
                        self.logger.info(f"Da dat gioi han {self.JOB_LIMIT} job thanh cong. Dung cao chi tiet.") 
                        break # Dung vong 'for idx, (link, job_id)'

                    # Tam nghi
                    if success_count % self.JOBS_PER_LONG_BREAK == 0 and success_count < len(new_jobs_to_crawl) and success_count < self.JOB_LIMIT:
                        sleep_time = random.uniform(self.LONG_BREAK_DURATION_MIN, self.LONG_BREAK_DURATION_MAX)
                        self.logger.info(f"Nghi dai sau {success_count} job... Se tiep tuc sau {round(sleep_time/60, 1)} phut.") 
                        time.sleep(sleep_time)
                    else:
                        time.sleep(random.uniform(self.PAUSE_BETWEEN_JOBS_MIN, self.PAUSE_BETWEEN_JOBS_MAX))
                
                except Exception as e:
                    error_count += 1
                    self.logger.error(f"Loi NGHIEM TRONG khi xu ly link {idx}/{len(new_jobs_to_crawl)} (ID: {job_id}): {link} | {e}") 
                    try:
                        driver.get(self.base_url)
                        time.sleep(5)
                    except Exception as e_nav:
                        self.logger.error(f"Loi khi dieu huong ve trang chu. Khoi dong lai driver... {e_nav}")
                        driver.quit()
                        time.sleep(5)
                        driver = self._create_driver()

        driver.quit()
        
    
        end_time = time.time()
        total_minutes = round((end_time - start_time) / 60, 2)
        
        # LOGIC XU LY FILE (Xoa file neu rong)
        if success_count > 0:
            self.logger.info(f"Crawl xong [{self.category_name}] trong {total_minutes} phut - Da luu {success_count} job MOI, Loi: {error_count}")
            return os.path.basename(output_file) 
        
        self.logger.info(f"Crawl xong [{self.category_name}] trong {total_minutes} phut - Khong co job moi. Loi: {error_count}")
        if os.path.exists(output_file):
            try:
                os.remove(output_file) 
                self.logger.info(f"Da xoa file CSV rong: {os.path.basename(output_file)}")
            except Exception as e:
                self.logger.error(f"Khong the xoa file rong {os.path.basename(output_file)}: {e}")
        
        return None

# ==================================================
# KHOI CODE DE CHAY DOC LAP
# ==================================================
if __name__ == '__main__':
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    
    categories_to_run = [
        ("IT_Software", "https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"),
        
    ]

    print(f"--- [BAT DAU] Dang chay CareerLink Scraper cho {len(categories_to_run)} danh muc ---")
    
    for category_name, base_url in categories_to_run:
        print(f"\n--- Dang xu ly danh muc: {category_name} ---")
        try:
            scraper = CareerLinkScraper(category_name, base_url)
            scraper.run()
        except Exception as e:
            print(f"!!! LOI NGHIEM TRONG (CareerLink - {category_name}): {e}")
    
    print("--- [HOAN TAT] CareerLink Scraper ---")