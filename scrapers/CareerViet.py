import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time, random, csv, os, re, sys, logging, math 
from datetime import datetime


# Them project root vao sys.path de chay doc lap
project_root_for_import = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root_for_import)


class CareerVietScraper:
    JOB_LIMIT = 10
    
    def __init__(self, category_name, base_url):
        """Khoi tao scraper cho mot danh muc CareerViet."""
        self.category_name = category_name
        self.base_url = base_url # Phai la URL da sort by date (sortdv)
        self.SOURCE_WEB = "CareerViet"

        # Cau hinh chung
        self.PAUSE_BETWEEN_PAGES_MIN = 3
        self.PAUSE_BETWEEN_PAGES_MAX = 6
        self.PAUSE_BETWEEN_JOBS_MIN = 4
        self.PAUSE_BETWEEN_JOBS_MAX = 8
        
        # [THEM MOI] So luong job tren mot trang (de tinh max_page)
        self.JOBS_PER_PAGE = 50
        
        # Thiet lap duong dan
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.scraper_dir = os.path.dirname(os.path.abspath(__file__)) 

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        self.seen_links_file = os.path.join(self.scraper_dir, f"seen_links_{self.category_name}.txt") 
        self.seen_links = set() 
        
        self.CSV_HEADER = [
            "CongViec", "ViTri", "YeuCauKinhNghiem", "MucLuong",
            "ThoiGianLamViec", "GioiTinh", "CapBac", "HinhThucLamViec", "CongTy", "LinkCongTy",
            "QuyMoCongTy", "SoLuongTuyen", "HocVan",
            "YeuCauUngVien", "MoTaCongViec", "QuyenLoi", "HanNopHoSo", "LinkBaiTuyenDung", "Nguon","NgayCaoDuLieu",
            "LinhVuc" 
        ]

        
        self.logger = logging.getLogger(f"CareerViet.{category_name}")
        self._setup_logging() 
        
        self.driver = None

    def _setup_logging(self): 
        base_logger = logging.getLogger(self.SOURCE_WEB)
        base_logger.setLevel(logging.INFO)

        if not base_logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            shared_log_path = os.path.join(self.scraper_dir, "CareerViet_SHARED.log")
            fh = logging.FileHandler(shared_log_path, encoding="utf-8")
            fh.setFormatter(formatter)
            base_logger.addHandler(fh)
            
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            base_logger.addHandler(ch)
        
        self.logger = logging.getLogger(f"{self.SOURCE_WEB}.{self.category_name}")

    def _load_seen_links(self):
        """Tai lich su cac link da cao tu file."""
        try:
            if os.path.exists(self.seen_links_file):
                with open(self.seen_links_file, 'r', encoding='utf-8') as f:
                    self.seen_links = {line.strip() for line in f if line.strip()}
                self.logger.info(f"Da tai {len(self.seen_links)} links tu lich su: {os.path.basename(self.seen_links_file)}")
            else:
                self.logger.info("Khong tim thay file lich su. Day la lan chay dau tien.")
        except Exception as e:
            self.logger.error(f"Loi khi tai file lich su {self.seen_links_file}: {e}")

    def _save_seen_links(self):
        """Luu lich su cac link da cao (bao gom ca link moi) ra file."""
        try:
            with open(self.seen_links_file, 'w', encoding='utf-8') as f:
                for link in self.seen_links:
                    f.write(f"{link}\n")
            self.logger.info(f"Da luu {len(self.seen_links)} links vao lich su: {os.path.basename(self.seen_links_file)}")
        except Exception as e:
            self.logger.error(f"Loi khi luu file lich su {self.seen_links_file}: {e}")

    def _create_driver(self):
        """Khoi tao Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("start-maximized")
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e:
            self.logger.error(f"Loi khi khoi tao WebDriver: {e}")
            raise 

    def _human_like_scroll(self):
        """Cuon trang giong nguoi."""
        try:
            height = self.driver.execute_script("return document.body.scrollHeight")
            pos = 0
            while pos < height:
                step = random.randint(300, 700)
                pos = min(pos + step, height)
                self.driver.execute_script(f"window.scrollTo(0, {pos});")
                time.sleep(random.uniform(0.2, 0.7))
                height = self.driver.execute_script("return document.body.scrollHeight")
        except Exception:
            pass

    def _safe_text(self, by, selector):
        """Lay text an toan, tu dong loc 'Key: Value' -> 'Value'."""
        try:
            raw_text = self.driver.find_element(by, selector).text.strip()
            if ":" in raw_text:
                return raw_text.split(":")[-1].strip()
            return raw_text
        except:
            return ""
            
    def _get_full_section_text(self, title_text):
        """Lay toan bo text cua mot muc (Mo ta, Yeu cau)."""
        try:
            parent_div = self.driver.find_element(By.XPATH, f"//h2[contains(text(), '{title_text}')]/parent::div[contains(@class, 'detail-row')]")
            raw_text = parent_div.text.strip()
            return raw_text.replace(title_text, "").strip()
        except:
            return ""

    def _get_company_info(self):
        company_name, company_link, company_size = "", "", ""
        try:
            tab_overview_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@data-href='#tab-2']"))
            )
            self.driver.execute_script("arguments[0].click();", tab_overview_button)
            self.logger.debug("Da click tab 'Tong quan', doi vai giay cho JS...")
            time.sleep(random.uniform(1.0, 1.5)) 
            WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "div.info"))
                )
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e_tab:
            self.logger.warning(f"Khong the click hoac tim thay tab 'Tong quan' (tab-2). Loi: {e_tab}")
            return company_name, company_link, company_size
        
        try:
            company_elem = self.driver.find_element(By.CSS_SELECTOR, "#tab-2 div.img div.title-company a.name")
            company_name = company_elem.get_attribute("title").strip()
            company_link = company_elem.get_attribute("href")
        except Exception as e:
            self.logger.warning(f"Khong lay duoc ten/link cong ty (trong tab 2): {e}")
        
        try:
            company_size_element = self.driver.find_element(
                By.XPATH,
                "//li[contains(., 'Quy mô công ty')]"
            )
            company_size_text = company_size_element.text  
            company_size = company_size_text.split(':')[-1].strip()
        except Exception:
            company_size = None
            
        return company_name, company_link, company_size

    def _build_page_url(self, page):
        """Tao URL phan trang cho CareerViet."""
        if page == 1:
            return self.base_url
        if self.base_url.endswith("-vi.html"):
            base, ext = self.base_url.split("-vi.html")
            return f"{base}-trang-{page}-vi.html"
        else:
            return f"{self.base_url}?page={page}" # Fallback

    # ==================================================
    # [THAY DOI] HAM TIM MAX PAGE (LOGIC MOI)
    # ==================================================
    def _get_max_page(self):
        """
        [LOGIC MOI] Tim max_page bang cach lay Tong So Job / Job Tren Mot Trang.
        """
        try:
            # Tai trang 1 (la self.base_url)
            self.logger.info(f"Dang tai trang 1 ({self.base_url}) de xac dinh so trang toi da (max_page)...")
            self.driver.get(self.base_url)
            
            # Doi cho tieu de (chua tong so job) xuat hien
            # Vi du: "1048 việc làm CNTT - Phần mềm..."
            title_element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//h1[contains(text(), 'việc làm')]"))
            )
            time.sleep(1)
            
            title_text = title_element.text.strip() # "1048 việc làm..."
            self.logger.debug(f"Tim thay Tieu de: '{title_text}'")
            
            # Dung regex de lay so dau tien (tong so job)
            match = re.search(r'^(\d+)', title_text)
            
            if match:
                total_jobs = int(match.group(1))
                # Tinh toan max_page
                max_page_found = math.ceil(total_jobs / self.JOBS_PER_PAGE)
                
                self.logger.info(f"Tim thay {total_jobs} jobs. Tinh toan max page la: {max_page_found} (voi {self.JOBS_PER_PAGE} jobs/page)")
                return max_page_found
            else:
                self.logger.warning(f"Khong tim thay tong so job tu Tieu de: '{title_text}'. Dat max_page = 1.")
                return 1

        except (TimeoutException, NoSuchElementException):
            self.logger.warning("Khong tim thay tieu de chua tong so job. Co the khong co job nao. Dat max_page = 1.")
            return 1 # Tra ve 1 neu khong tim thay
        except Exception as e:
            self.logger.error(f"Loi khong xac dinh khi tim max page: {e}. Dat max_page = 1.")
            return 1
            
    # ==================================================
    # HAM RUN CHINH (Tu dong quet)
    # ==================================================
    def run(self):
        
        start_time = time.time()
        self.logger.info(f"Bat dau phien cao du lieu [{self.category_name}] (Gioi han {self.JOB_LIMIT} job moi)...")
        
        self._create_driver()
        if not self.driver:
            return

        # B1: Tai lich su
        self.logger.info("Dang tai lich su links...")
        self._load_seen_links()

        # ==========================================================
        # [THAY DOI] Logic tim max page
        # ==========================================================
        try:
            # Ham nay se tai trang 1 va tinh toan max_page
            max_page_limit = self._get_max_page() 
        except Exception as e:
            self.logger.error(f"Gap loi nghiem trong khi lay max_page: {e}. Dung quet trang.")
            self.driver.quit()
            return None # Thoat som neu khong lay duoc max_page

        # ===== VONG 1: THU THAP LINK (Tu dong quet trang) =====
        self.logger.info(f"Bat dau quet tu trang 1 den {max_page_limit}...")
        
        new_jobs_to_crawl = []
        jobs_collected_count = 0 
        stop_collecting = False
        
        page = 1
        consecutive_pages_with_no_new_jobs = 0
        MAX_CONSECUTIVE_EMPTY_PAGES = 5 

        while page <= max_page_limit:
            if stop_collecting:
                self.logger.info(f"Da du {self.JOB_LIMIT} job moi. Dung quet trang.")
                break 

            if consecutive_pages_with_no_new_jobs >= MAX_CONSECUTIVE_EMPTY_PAGES:
                self.logger.info(f"Dung quet vi {MAX_CONSECUTIVE_EMPTY_PAGES} trang lien tiep khong co job MOI nao (mac du max_page la {max_page_limit}).")
                break 
            
            job_cards = [] 
            
            # ===== Lấy link việc làm =====
            try:
                if page == 1:
                    self.logger.info(f"Dang xu ly trang {page}/{max_page_limit} (da duoc tai de lay max_page)...")
                    self._human_like_scroll()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-item"))
                    )
                else:
                    url = self._build_page_url(page)
                    self.logger.info(f"Dang quet trang {page}/{max_page_limit}...")
                    self.driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    self._human_like_scroll()
                    
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-item"))
                    )
                
                job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job-item")
                
            except Exception as e:
                self.logger.warning(f"Trang {page} khong load duoc hoac khong tim thay job card. Bo qua trang. Loi: {e}") 
                page += 1 
                continue 

            if not job_cards:
                self.logger.warning(f"Trang {page} khong co job nao. Chuyen trang tiep theo.") 
                page += 1 
                consecutive_pages_with_no_new_jobs += 1 
                continue 

            new_jobs_found_on_page = 0
            for card in job_cards:
                if stop_collecting:
                    break 

                try:
                    
                    link_element = card.find_element(By.CSS_SELECTOR, "a.job_link") 
                    link_job = link_element.get_attribute("href")

                    
                    if link_job:
                        if link_job not in self.seen_links:
                            
                            company_name_card = ""
                            company_link_card = ""
                            try:
                                company_elem_card = card.find_element(By.CSS_SELECTOR, "a.company-name")
                                company_name_card = company_elem_card.get_attribute("title").strip() 
                                company_link_card = company_elem_card.get_attribute("href")
                            except Exception:
                                self.logger.debug(f"Khong tim thay ten/link cong ty tren card cho link: {link_job}")
                            
                            new_jobs_to_crawl.append((link_job, company_name_card, company_link_card))
                            
                            jobs_collected_count += 1
                            new_jobs_found_on_page += 1
                            
                            if jobs_collected_count >= self.JOB_LIMIT:
                                stop_collecting = True
                                
                except Exception as e_link:
                    self.logger.warning(f"Khong tim thay link trong mot job card. Loi: {e_link}")
                    continue
            
            if new_jobs_found_on_page > 0:
                self.logger.info(f"Trang {page} -> Tim thay {new_jobs_found_on_page} job MOI. (Tong so job moi: {jobs_collected_count}/{self.JOB_LIMIT})") 
                consecutive_pages_with_no_new_jobs = 0 
            else:
                self.logger.info(f"Trang {page} khong co job moi nao.") 
                consecutive_pages_with_no_new_jobs += 1 

            
            page += 1 # Chuyen sang trang tiep theo
            time.sleep(random.uniform(self.PAUSE_BETWEEN_PAGES_MIN, self.PAUSE_BETWEEN_PAGES_MAX))
        
        self.logger.info(f"Hoan thanh quet trang (da quet den trang {page-1} / gioi han {max_page_limit}).")

        self.logger.info(f"Da thu thap xong. Co {len(new_jobs_to_crawl)} job MOI can cao chi tiet.")

        # ===== VONG 2: CAO CHI TIET =====
        
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"CareerViet_{self.category_name}_jobs_{now_str}.csv")
        self.logger.info(f"Du lieu se duoc luu vao file: {os.path.basename(output_file)}")

        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)

        success_count, error_count = 0, 0
        
        if new_jobs_to_crawl: # Chi chay neu co job moi
            with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                
                for idx, (link, company_name_card, company_link_card) in enumerate(new_jobs_to_crawl, 1):
                    if success_count >= self.JOB_LIMIT:
                        self.logger.info(f"Da dat gioi han {self.JOB_LIMIT} job thanh cong. Dung cào chi tiet.")
                        break
                        
                    try:
                        self.driver.get(link)
                        time.sleep(random.uniform(2, 5))
                        self._human_like_scroll()
                        WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.detail-row"))) 
                        
                        # Khoi tao du 22 bien
                        title, work_location, experience, salary = "", "", "", ""
                        work_time, gender, level, work_form = "", "", "", ""
                        
                        company_name, company_link, company_size = company_name_card, company_link_card, ""
                        
                        recruit_quantity, education = "", ""
                        requirement, job_description, benefits = "", "", ""
                        deadline = ""
                        linh_vuc = "" 
                        link_job = link
                        source_web = self.SOURCE_WEB
                        ngay_cao_hien_tai = datetime.now().strftime('%Y-%m-%d')
                        
                        # Cao du lieu
                        title = self._safe_text(By.CSS_SELECTOR, "h1.title")
                        linh_vuc = self._safe_text(By.XPATH, "//strong[contains(., 'Ngành nghề')]/following-sibling::p")
                        work_location = self._safe_text(By.XPATH, "//strong[contains(.,'Địa điểm')]/following-sibling::p/a")
                        experience = self._safe_text(By.XPATH, "//strong[contains(.,'Kinh nghiệm')]/following-sibling::p")
                        salary = self._safe_text(By.XPATH, "//strong[contains(.,'Lương')]/following-sibling::p | //li[contains(text(), 'Lương:')]")
                        level = self._safe_text(By.XPATH, "//strong[contains(.,'Cấp bậc')]/following-sibling::p")
                        work_form = self._safe_text(By.XPATH, "//strong[contains(.,'Hình thức')]/following-sibling::p")
                        deadline = self._safe_text(By.XPATH, "//strong[contains(.,'Hết hạn nộp')]/following-sibling::p")
                        education = self._safe_text(By.XPATH, "//li[contains(text(), 'Bằng cấp:')]")
            
                        requirement = self._get_full_section_text("Yêu Cầu Công Việc")
                        job_description = self._get_full_section_text("Mô tả Công việc")
                        
                        try:
                            benefits_elem = self.driver.find_element(By.XPATH, "//h2[contains(text(), 'Phúc lợi')]/following-sibling::ul[@class='welfare-list']")
                            benefits = benefits_elem.text.strip()
                        except Exception:
                            benefits = ""

                        
                        
                        
                        
                        name_from_tab2, link_from_tab2, size_from_tab2 = self._get_company_info()
                        
                        # Hop nhat du lieu: Uu tien thong tin tu Card, nhung lay Size tu Tab2
                        company_name = company_name if company_name else name_from_tab2
                        company_link = company_link if company_link else link_from_tab2
                        company_size = size_from_tab2 # Luon lay size moi tu tab (neu co)
                        
                        # Ghi du 22 cot vao CSV
                        writer.writerow([
                            title, work_location, experience, salary,
                            work_time, gender, level, work_form,
                            company_name, company_link, company_size, recruit_quantity, education,
                            requirement, job_description, benefits,
                            deadline, link_job, source_web, ngay_cao_hien_tai,
                            linh_vuc
                        ])
                        
                        self.seen_links.add(link)
                        success_count += 1
                        
                        self.logger.info(f"[{success_count}/{len(new_jobs_to_crawl)}] Da cao va luu job: {title[:60]}...") 
                        
                        
                        time.sleep(random.uniform(self.PAUSE_BETWEEN_JOBS_MIN, self.PAUSE_BETWEEN_JOBS_MAX))
                    
                    except Exception as e:
                        error_count += 1
                        
                        self.logger.error(f"Loi NGHIEM TRONG khi xu ly link {idx}/{len(new_jobs_to_crawl)}: {link} | {e}") 
                        
                        continue 
        
        # Luu lai toan bo lich su (cu + moi) ra file
        self._save_seen_links()
        
        self.driver.quit()
        
        # ===== XU LY KET THUC =====
        end_time = time.time()
        total_minutes = round((end_time - start_time) / 60, 2)
        
        if success_count > 0:
            
            self.logger.info(f"Crawl xong [{self.category_name}] trong {total_minutes} phut - Da luu {success_count} job, Loi: {error_count}")
        else:
            self.logger.info(f"Crawl xong [{self.category_name}] trong {total_minutes} phut - Khong co job nao MOI. Loi: {error_count}")
            if os.path.exists(output_file):
                try:
                    os.remove(output_file) 
                    self.logger.info(f"Da xoa file CSV rong: {os.path.basename(output_file)}")
                except Exception as e:
                    self.logger.error(f"Khong the xoa file rong {os.path.basename(output_file)}: {e}")
                        
# ==================================================
# KHOI CODE DE CHAY DOC LAP (Tu dong quet)
# ==================================================
if __name__ == '__main__':
    
    # --- Cau hinh URL ---
    url_1 = "https://careerviet.vn/viec-lam/cntt-phan-mem-c1-sortdv-vi.html"
    
    
    
    
    print(f"\n--- BAT DAU PHIEN CAO MOI (MAX {CareerVietScraper.JOB_LIMIT} JOBS/CATEGORY) ---")
    print(f"--- SE TU DONG QUET TU TRANG 1 ---")
    try:
        print("\n--- Dang xu ly danh muc: IT_Software ---")
        scraper_sw = CareerVietScraper("IT_Software", url_1) 
        scraper_sw.run()
    except Exception as e:
        print(f"!!! LOI NGHIEM TRONG (CareerViet - IT_Software): {e}")
        logging.exception("Loi nghiem trong IT_Software") 
    print("\n--- [HOAN TAT] CareerViet Scraper ---")