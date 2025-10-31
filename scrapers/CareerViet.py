# scrapers/careerviet_scraper.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time, random, csv, os, datetime, re

class CareerVietScraper:
    def __init__(self, category_name, base_url):
        """Khởi tạo scraper cho một danh mục cụ thể trên CareerViet."""
        self.category_name = category_name
        self.base_url = base_url
        self.SOURCE_WEB = "CareerViet"

        # ===== CẤU HÌNH CHUNG =====
        self.PAUSE_BETWEEN_PAGES_MIN = 3
        self.PAUSE_BETWEEN_PAGES_MAX = 6
        self.PAUSE_BETWEEN_JOBS_MIN = 4
        self.PAUSE_BETWEEN_JOBS_MAX = 8
        self.JOBS_PER_LONG_BREAK = 50
        self.LONG_BREAK_DURATION_MIN = 60
        self.LONG_BREAK_DURATION_MAX = 120

        # ===== CẤU HÌNH QUÉT TRANG (GIỐNG TOPCV) =====
        self.START_PAGE = 1
        self.PAGES_TO_ADD_PER_RUN = 2 # Số trang sẽ cộng thêm cho lần chạy kế tiếp

        # ===== THIẾT LẬP ĐƯỜNG DẪN =====
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scraper_dir = os.path.dirname(os.path.abspath(__file__))

        self.csv_output_dir = os.path.join(project_root, "dataset")
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        self.log_file = os.path.join(scraper_dir, "CareerViet_log.txt")
        self.id_history_file = os.path.join(scraper_dir, "CareerViet_id_history.txt")
        # File để lưu trang tối đa đã quét
        self.max_page_file = os.path.join(scraper_dir, f"CareerViet_{self.category_name}_max_page.txt")


        self.CSV_HEADER = [
            "title", "work_location", "salary", "experience", "level", "work_form", "company_name", "company_link",
            "company_size", "gender", "education", "age", "careers_field", "skills", "job_description","requirement", "benefits",
            "post_date", "deadline", "link", "source_web"
        ]

    def _create_driver(self):
        """Tạo và trả về một instance của Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--headless=new")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def _write_log(self, message):
        """Ghi log có định dạng."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{now}] [{self.category_name}] {message}\n"
        with open(self.log_file, "a", encoding="utf-8") as log:
            log.write(log_message)
        print(log_message.strip())

    def _get_existing_ids(self):
        """Đọc và trả về một set các ID đã cào từ trước."""
        if not os.path.exists(self.id_history_file): return set()
        try:
            with open(self.id_history_file, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            self._write_log(f"Lỗi khi đọc file lịch sử ID: {e}")
            return set()

    def _extract_job_id_from_link(self, link):
        """Trích xuất ID từ link job của CareerViet."""
        if not link: return None
        match = re.search(r'-(\w+)\.html', link)
        return match.group(1) if match else None

    def _human_like_scroll(self, driver):
        """Cuộn trang một cách tự nhiên."""
        try:
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            for i in range(0, scroll_height, random.randint(300, 500)):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(random.uniform(0.3, 0.7))
        except Exception as e:
            self._write_log(f"Lỗi khi cuộn trang: {e}")

    # --- Helper methods for scraping details ---
    def _get_text_by_label(self, driver, label):
        try:
            xpath = f"//strong[contains(., '{label}')]/following-sibling::p"
            info = driver.find_element(By.XPATH, xpath).text
            return " ".join(info.split())
        except NoSuchElementException:
            return ""

    def _get_other_info(self, driver, label):
        try:
            xpath = f"//h3[text()='Thông tin khác']/following-sibling::div//li[contains(., '{label}')]"
            li_element = driver.find_element(By.XPATH, xpath)
            full_text = li_element.text
            value = full_text.replace(label, '').replace(':', '').strip()
            return value
        except NoSuchElementException:
            return ""
            
    def run(self):
        """Phương thức chính để chạy toàn bộ quá trình cào dữ liệu."""
        self._write_log("🚀 Bắt đầu phiên cào dữ liệu CareerViet mới...")
        
        now_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.csv_output_dir, f"CareerViet_{self.category_name}_jobs_{now_str}.csv")
        self._write_log(f"📄 Dữ liệu lần này sẽ được lưu vào file: {os.path.basename(output_file)}")
        
        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)

        # ===== LOGIC ĐỌC/GHI SỐ TRANG TỐI ĐA =====
        try:
            if not os.path.exists(self.max_page_file):
                max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                with open(self.max_page_file, 'w') as f: f.write(str(max_page_to_crawl))
                self._write_log(f"File max_page.txt không tồn tại. Tạo mới và đặt trang tối đa là {max_page_to_crawl}.")
            else:
                with open(self.max_page_file, 'r') as f:
                    content = f.readline().strip()
                    if content and content.isdigit():
                        max_page_to_crawl = int(content)
                    else:
                        max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
                        self._write_log(f"Nội dung file max_page.txt không hợp lệ. Đặt lại trang tối đa là {max_page_to_crawl}.")
        except Exception as e:
            max_page_to_crawl = self.PAGES_TO_ADD_PER_RUN
            self._write_log(f"Lỗi khi đọc file max_page.txt: {e}. Đặt lại trang tối đa là {max_page_to_crawl}.")

        self._write_log(f"📌 Lần này sẽ quét từ trang {self.START_PAGE} → {max_page_to_crawl}.")
        
        driver = self._create_driver()
        existing_ids = self._get_existing_ids()
        self._write_log(f"📊 Đã tìm thấy {len(existing_ids)} ID jobs trong lịch sử chung của CareerViet.")
        
        # --- Thu thập link của các job mới ---
        new_jobs_to_crawl = []
        for page in range(self.START_PAGE, max_page_to_crawl + 1):
            base_link_part = self.base_url.split('.html')[0]
            url = f"{base_link_part}-trang-{page}-vi.html"
            
            self._write_log(f"🔎 Đang quét trang {page}/{max_page_to_crawl}: {url}")
            try:
                driver.get(url)
                self._human_like_scroll(driver)
                WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.job-item has-background")))
            except TimeoutException:
                 self._write_log(f"⚠️ Trang {page} không load được hoặc không có job (hết trang). Dừng quét link.")
                 break # Dừng vòng lặp nếu hết trang
            except Exception as e:
                self._write_log(f"⚠️ Lỗi khi tải trang {page}. Bỏ qua. Lỗi: {e}")
                continue

            job_cards = driver.find_elements(By.CSS_SELECTOR, "div.job-item.has-background")
            
            new_jobs_found_on_page = 0
            # 2. Lặp qua từng card
            for card in job_cards:
                try:
                    # 3. Tìm link bên trong card và lấy href
                    link_element = card.find_element(By.CSS_SELECTOR, "a.job_link")
                    link_job = link_element.get_attribute("href")
                    
                    job_id = self._extract_job_id_from_link(link_job)
                    if job_id and job_id not in existing_ids:
                        new_jobs_to_crawl.append((link_job, job_id))
                        existing_ids.add(job_id)
                        new_jobs_found_on_page += 1
                except Exception:
                    continue
            
            if new_jobs_found_on_page > 0:
                self._write_log(f"Trang {page} → Tìm thấy {new_jobs_found_on_page} job MỚI.")
            else:
                self._write_log(f"Trang {page} không có job nào mới.")
            
            time.sleep(random.uniform(self.PAUSE_BETWEEN_PAGES_MIN, self.PAUSE_BETWEEN_PAGES_MAX))

        self._write_log(f"🎉 Đã thu thập xong. Có {len(new_jobs_to_crawl)} job mới cần cào chi tiết.")
        
        # --- Cào chi tiết từng job ---
        success_count, error_count = 0, 0
        if not new_jobs_to_crawl:
            self._write_log("Không có job mới nào để cào. Kết thúc.")
        else:
            for idx, (link, job_id) in enumerate(new_jobs_to_crawl, 1):
                try:
                    driver.get(link)
                    wait = WebDriverWait(driver, 20)
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-detail")))
                    self._human_like_scroll(driver)
                    
                    # Dọn dẹp logic cào chi tiết
                    title = driver.find_element(By.CSS_SELECTOR, "h1.title").text.strip()
                    location_div = wait.until(EC.visibility_of_element_located((By.XPATH, "//h3[text()='Địa điểm làm việc']/following-sibling::div")))
                    work_location = ", ".join(location_div.text.split('\n'))

                    salary = self._get_text_by_label(driver, "Lương")
                    level = self._get_text_by_label(driver, "Cấp bậc")
                    deadline = self._get_text_by_label(driver, "Hết hạn nộp")
                    experience = self._get_text_by_label(driver, "Kinh nghiệm")
                    post_date = self._get_text_by_label(driver, "Ngày cập nhật")
                    work_form = self._get_text_by_label(driver, "Hình thức")

                    education = self._get_other_info(driver, "Bằng cấp")
                    gender = self._get_other_info(driver, "Giới tính")
                    age = self._get_other_info(driver, "Độ tuổi")

                    # Lấy mô tả, yêu cầu, quyền lợi
                    desc_elements = driver.find_elements(By.XPATH, "//h2[text()='Mô tả Công việc']/following-sibling::*")
                    job_description = "\n".join([el.text for el in desc_elements if el.tag_name == 'p'])
                    
                    req_elements = driver.find_elements(By.XPATH, "//h2[text()='Yêu Cầu Công Việc']/following-sibling::p[count(preceding-sibling::p/strong[contains(.,'QUYỀN LỢI')])=0]")
                    requirement = "\n".join([p.text for p in req_elements if p.text.strip()])
                    
                    benefit_elements = driver.find_elements(By.XPATH, "//p[strong[contains(.,'QUYỀN LỢI')]]/following-sibling::p[count(following-sibling::p/strong[contains(.,'PHÚC LỢI')]) > 0]")
                    benefits = "\n".join([p.text for p in benefit_elements if p.text.strip()])

                    # Lấy careers và skills
                    career_elements = driver.find_elements(By.XPATH, "//strong[contains(., 'Ngành nghề')]/following-sibling::p//a")
                    careers_list = [" ".join(c.text.split()) for c in career_elements if c.text.strip()]
                    careers_field = ", ".join(careers_list)

                    skill_elements = driver.find_elements(By.CSS_SELECTOR, "div.job-tags a")
                    skills_list = [skill.text.strip() for skill in skill_elements if skill.text.strip()]
                    skills_str = ", ".join(skills_list)
                    
                    # Lấy thông tin công ty
                    company_name, company_link, company_size = "", "", ""
                    try:
                        company_tab = wait.until(EC.element_to_be_clickable((By.ID, "tabs-job-company")))
                        company_tab.click()
                        time.sleep(1) # Chờ tab load
                        comp_element = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.title-company a.name")))
                        company_name = comp_element.text
                        company_link = comp_element.get_attribute('href')
                        size_element = driver.find_element(By.XPATH, "//li[contains(., 'Quy mô công ty')]")
                        company_size = size_element.text.split(':')[-1].strip()
                    except Exception:
                        self._write_log(f"Không tìm thấy thông tin công ty cho job ID {job_id}")

                    # Ghi vào CSV
                    with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            title, work_location, salary, experience, level, work_form,
                            company_name, company_link, company_size, gender, education, age,
                            careers_field, skills_str, job_description,requirement, benefits, post_date,
                            deadline, link, self.SOURCE_WEB])
                    
                    with open(self.id_history_file, "a", encoding="utf-8") as f:
                        f.write(job_id + "\n")

                    success_count += 1
                    self._write_log(f"✅ [{success_count}/{len(new_jobs_to_crawl)}] Đã cào và lưu job ID {job_id}: {title[:60]}...")
                    
                    # Tạm nghỉ
                    if success_count % self.JOBS_PER_LONG_BREAK == 0 and success_count < len(new_jobs_to_crawl):
                        sleep_time = random.uniform(self.LONG_BREAK_DURATION_MIN, self.LONG_BREAK_DURATION_MAX)
                        self._write_log(f"⏸ Nghỉ dài sau {success_count} job... Sẽ tiếp tục sau {round(sleep_time/60, 1)} phút.")
                        time.sleep(sleep_time)
                    else:
                        time.sleep(random.uniform(self.PAUSE_BETWEEN_JOBS_MIN, self.PAUSE_BETWEEN_JOBS_MAX))
                
                except Exception as e:
                    error_count += 1
                    self._write_log(f"❌ Lỗi khi xử lý link {idx}/{len(new_jobs_to_crawl)} (ID: {job_id}): {link} | {e}")
                    continue
        
        # ===== CẬP NHẬT FILE MAX_PAGE CHO LẦN CHẠY TIẾP THEO =====
        new_max_page = max_page_to_crawl + self.PAGES_TO_ADD_PER_RUN
        try:
            with open(self.max_page_file, "w") as f: f.write(str(new_max_page))
            self._write_log(f"🔄 Đã cập nhật max_page.txt cho lần chạy tiếp theo: {new_max_page}")
        except Exception as e:
            self._write_log(f"❌ Không thể cập nhật file max_page.txt: {e}")

        driver.quit()
        self._write_log(f"🏁 Crawl xong - Đã lưu {success_count} job MỚI, Lỗi: {error_count}")