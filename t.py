import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time, random, csv, os, datetime, sys, re

# ==========================================================
# ===== CẤU HÌNH CHO PIPELINE (VIETNAMWORKS ?g=5) =====
# ==========================================================
# Số trang SẼ CÀO THÊM cho lần chạy kế tiếp
PAGES_TO_ADD_PER_RUN = 2 
JOBS_PER_BREAK = 50
BREAK_DURATION_MIN = 120
BREAK_DURATION_MAX = 300
BATCH_SIZE = 50 # Số job cào xong thì restart driver

# --- Thông tin riêng của trang này ---
TARGET_URL = "https://www.vietnamworks.com/viec-lam?g=5"
SOURCE_WEB_NAME = "VietnamWorks_g5" # Tên để lưu vào cột 'source_web'

# --- Các Selector (Lấy từ code chúng ta vừa test) ---
JOB_CARD_SELECTOR = "div.view_job_item.new-job-card" 
LINK_SELECTOR_INSIDE_CARD = "h2 a"
NEXT_BUTTON_XPATH = "//ul[contains(@class, 'pagination')]//li/button[text()='>']"

# ===== Hàm setup Chrome (Không thay đổi) =====
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # chrome_options.add_argument("--headless=new") # Bật lại dòng này khi chạy trên server
    return webdriver.Chrome(options=chrome_options)

# ===== Các hàm setup file, log =====
# Thư mục gốc (Đổi tên để không đè lên TopCV)
output_dir = "VietnamWorks_g5"
os.makedirs(output_dir, exist_ok=True)

# Thư mục con để lưu CSV
csv_output_dir = os.path.join(output_dir, "VNW_g5_csv")
os.makedirs(csv_output_dir, exist_ok=True)

# Các file quản lý trạng thái
log_file = os.path.join(output_dir, "VNW_g5_log.txt")
id_history_file = os.path.join(output_dir, "id_jobhistory.txt") 
# File này sẽ lưu số trang tối đa sẽ cào (ví dụ: 3, 5, 7, 9...)
max_page_file = os.path.join(output_dir, "max_pages_to_crawl.txt")

# Tạo file CSV mới cho mỗi lần chạy
now_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file = os.path.join(csv_output_dir, f"VNW_g5_jobs_{now_str}.csv")

# Danh sách các cột (HEADER) - Lấy theo yêu cầu của bạn
CSV_HEADER = [
    "title", "work_location", "experience", "salary",
    "work_time", "level", "work_form", "company_name", "company_link",
    "company_size", "recruit_quantity", "education",
    "requirement", "job_description", "benefits", "deadline", "link", "source_web"
]

# Khởi tạo file CSV
with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(CSV_HEADER)

def write_log(message):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as log:
        log.write(f"[{now}] {message}\n")
    print(message)

# ===== Các hàm xử lý ID (Không thay đổi) =====
def get_existing_ids(file_path):
    """Đọc file lịch sử và trả về một SET chứa các ID đã cào."""
    if not os.path.exists(file_path):
        return set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        write_log(f"Lỗi khi đọc file lịch sử ID {file_path}: {e}")
        return set()

def extract_job_id_from_link(link):
    """Trích xuất chuỗi số ID từ cuối URL (Hoạt động cho cả TopCV và VNW)."""
    if not link:
        return None
    match = re.search(r'/(\d+)\.html', link)
    if match:
        return match.group(1)
    # Thêm dự phòng cho link VNW có ?jv=
    match_jv = re.search(r'-(\d+)-jv', link)
    if match_jv:
        return match_jv.group(1)
    write_log(f"WARNING: Không thể trích xuất ID từ link: {link}")
    return None

# ===== Hàm hỗ trợ lấy text an toàn =====
def get_safe(driver, selector, by=By.CSS_SELECTOR):
    """Lấy .text của element, trả về "" nếu không tìm thấy."""
    try:
        return driver.find_element(by, selector).text.strip()
    except:
        return ""

def get_safe_attr(driver, selector, attribute, by=By.CSS_SELECTOR):
    """Lấy attribute của element, trả về "" nếu không tìm thấy."""
    try:
        return driver.find_element(by, selector).get_attribute(attribute)
    except:
        return ""


# ==========================================================
# ===== BẮT ĐẦU CHƯƠNG TRÌNH CHÍNH =====
# ==========================================================
start_time = time.time()
write_log(f"🚀 Bắt đầu phiên cào dữ liệu {SOURCE_WEB_NAME} mới...")
write_log(f"📄 Dữ liệu lần này sẽ được lưu vào file: {os.path.basename(output_file)}")

# Logic đọc và quản lý max_page_file.txt (Giống TopCV)
try:
    if not os.path.exists(max_page_file):
        # Lần đầu tiên chạy, đặt số trang cào = số trang thêm mỗi lần (ví dụ: 2)
        max_pages_to_crawl_this_run = PAGES_TO_ADD_PER_RUN 
        with open(max_page_file, 'w') as f:
            f.write(str(max_pages_to_crawl_this_run))
        write_log(f"File {max_page_file} không tồn tại. Tạo mới và đặt số trang cào là {max_pages_to_crawl_this_run}.")
    else:
        with open(max_page_file, 'r') as f:
            content = f.readline().strip()
            if content and content.isdigit():
                max_pages_to_crawl_this_run = int(content)
            else:
                max_pages_to_crawl_this_run = PAGES_TO_ADD_PER_RUN
                write_log(f"Nội dung file {max_page_file} không hợp lệ. Đặt lại số trang cào là {max_pages_to_crawl_this_run}.")
except Exception as e:
    max_pages_to_crawl_this_run = PAGES_TO_ADD_PER_RUN
    write_log(f"Lỗi khi đọc file {max_page_file}: {e}. Đặt lại số trang cào là {max_pages_to_crawl_this_run}.")

write_log(f"📌 Lần này sẽ cào tối đa {max_pages_to_crawl_this_run} trang.")

driver = create_driver()

# Tải các ID đã cào vào một SET để kiểm tra nhanh
existing_ids = get_existing_ids(id_history_file)
write_log(f"📊 Đã tìm thấy {len(existing_ids)} ID jobs trong lịch sử.")

# =========================================================================
# B1: (Đã thay đổi) Thu thập link và ID MỚI bằng cách chuyển trang
# =========================================================================
new_jobs_to_crawl = [] # Sẽ chứa các tuple (link, job_id)
stop_crawling = False
current_page = 1

try:
    write_log(f"🔎 Đang truy cập trang: {TARGET_URL}")
    driver.get(TARGET_URL)
    
    # === Vòng lặp cào nhiều trang (Lấy từ code test) ===
    while current_page <= max_pages_to_crawl_this_run:
        if stop_crawling:
            break
            
        write_log(f"\n=========================================")
        write_log(f"🔎 BẮT ĐẦU QUÉT TRANG {current_page} 🔎")
        
        # 1. Chờ job card xuất hiện
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, JOB_CARD_SELECTOR))
            )
        except TimeoutException:
            write_log("❌ Hết giờ chờ, không thấy job card. Dừng lại.")
            break 

        # 2. Lấy tất cả job card
        job_cards = driver.find_elements(By.CSS_SELECTOR, JOB_CARD_SELECTOR)
        if not job_cards:
            write_log("✅ Không tìm thấy job card nào. Dừng.")
            break

        # 3. Lặp qua các card và KIỂM TRA ID (Logic của TopCV)
        new_jobs_found_on_page = 0
        for card in job_cards:
            try:
                link_element = card.find_element(By.CSS_SELECTOR, LINK_SELECTOR_INSIDE_CARD)
                link = link_element.get_attribute("href")
                job_id = extract_job_id_from_link(link)

                if job_id and job_id not in existing_ids:
                    new_jobs_to_crawl.append((link, job_id))
                    existing_ids.add(job_id) # Thêm ngay vào set để tránh trùng lặp
                    new_jobs_found_on_page += 1
            except Exception as e:
                write_log(f"Lỗi nhỏ khi lấy link/ID 1 card: {e}")
                continue
                
        write_log(f"Trang {current_page} → Tìm thấy {new_jobs_found_on_page} job MỚI.")

        # 4. Logic "Dừng thông minh" (Của TopCV)
        # Nếu trang này không có job mới (và đây không phải trang 1), thì dừng
        if new_jobs_found_on_page == 0 and current_page > 1:
            write_log(f"✅ Trang {current_page} không có job nào mới. Dừng thu thập link.")
            stop_crawling = True
            break
            
        # 5. Tìm và Click nút "Next" (Logic của VNW test)
        try:
            next_button = driver.find_element(By.XPATH, NEXT_BUTTON_XPATH)
            
            if next_button.is_enabled():
                write_log("🖱️ Đang click vào nút 'Next'...")
                next_button.click()
                current_page += 1 # Tăng số trang lên
                write_log("   -> Chờ trang mới tải (3 giây)...")
                time.sleep(random.uniform(3, 5))
            else:
                write_log("❌ Nút 'Next' đã bị mờ. Đây là trang cuối. Dừng.")
                break 

        except NoSuchElementException:
            write_log("❌ Không tìm thấy nút 'Next'. Đây là trang cuối. Dừng.")
            break # Thoát khỏi vòng lặp while

except Exception as e:
    write_log(f"Lỗi nghiêm trọng ở Giai đoạn 1: {e}")
    
write_log(f"🎉 Đã thu thập xong. Có {len(new_jobs_to_crawl)} job mới cần cào chi tiết.")


# ===============================================
# B2: Vào từng link lấy chi tiết và lưu trữ
# ===============================================
success_count, error_count = 0, 0
if not new_jobs_to_crawl:
    write_log("Không có job mới nào để cào. Kết thúc.")
else:
    write_log("--- BẮT ĐẦU CÀO CHI TIẾT ---")

    # =========================================================================
    # ===== THAY ĐỔI 1: GIỚI HẠN CHẠY 5 JOBS ĐỂ TEST =====
    # Chúng ta thêm [:5] để chỉ lấy 5 phần tử đầu tiên
    # =========================================================================
    write_log(f"--- !!! CHẾ ĐỘ TEST: CHỈ LẤY 5 JOBS ĐẦU TIÊN TỪNG {len(new_jobs_to_crawl)} JOBS MỚI TÌM THẤY ---")
    for idx, (link, job_id) in enumerate(new_jobs_to_crawl[:5], 1):
        try:
            driver.get(link)
            # Chờ một element đặc trưng của trang chi tiết
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.job-title")))
            time.sleep(random.uniform(2, 4))
            
            
            
            write_log(f"--- Đang cào ID: {job_id} ---")

            title = get_safe(driver, "h1.title")
            try:
                xpath_diadiem = "//h2[contains(text(), 'Địa điểm làm việc')]/following-sibling::div//p[@name='paragraph']"
                work_location = driver.find_element(By.XPATH, xpath_diadiem).text.strip()
            except:
                work_location = ""
            experience = get_safe(driver, "span.experience-value")
            salary = get_safe(driver, "span.salary-value")
            work_time = "" # VNW không có trường này
            level = get_safe(driver, "span.level-value")
            work_form = "" # VNW không có trường này
            company_name = get_safe(driver, "div.company-name")
            company_link = get_safe_attr(driver, "a.company-logo-wrapper", "href")
            company_size = get_safe(driver, "span.company-size-value")
            recruit_quantity = "" # VNW không có trường này
            education = "" # VNW không có trường này
            
            # Xử lý 3 khối text lớn
            requirement = ""
            job_description = ""
            benefits = ""
            try:
                full_text = driver.find_element(By.CSS_SELECTOR, "div.job-description").text
                # Tách Mô Tả CV
                desc_parts = re.split(r'(Yêu Cầu Công Việc|Yêu Cầu Ứng Viên|Requirements)', full_text, maxsplit=1, flags=re.IGNORECASE | re.MULTILINE)
                job_description = desc_parts[0].strip()
                
                if len(desc_parts) > 1:
                    remaining_text = desc_parts[2]
                    # Tách Yêu Cầu và Quyền Lợi
                    req_parts = re.split(r'(Quyền Lợi|Benefits|Phúc Lợi)', remaining_text, maxsplit=1, flags=re.IGNORECASE | re.MULTILINE)
                    requirement = req_parts[0].strip()
                    if len(req_parts) > 1:
                        benefits = req_parts[2].strip()
                
                # Fallback nếu không tách được
                if not job_description and not requirement and not benefits:
                    job_description = full_text
            except Exception:
                job_description = get_safe(driver, "div.job-description") # Fallback
            
            deadline = get_safe(driver, "span.expiration-date-value")
            link = link # Đã có sẵn
            source_web = SOURCE_WEB_NAME # Đã có sẵn
            
            
            # Dữ liệu được sắp xếp theo đúng thứ tự của CSV_HEADER
            job_data = [
                title, work_location, experience, salary,
                work_time, level, work_form, company_name, company_link,
                company_size, recruit_quantity, education,
                requirement, job_description, benefits, deadline, link, source_web
            ]
            
            # Ghi vào file CSV của lần chạy này
            with open(output_file, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(job_data)

            # Ghi ID vừa cào thành công vào file lịch sử
            with open(id_history_file, "a", encoding="utf-8") as f:
                f.write(job_id + "\n")

            success_count += 1
            write_log(f"✅ [{success_count}/{len(new_jobs_to_crawl[:5])}] Đã cào và lưu job ID {job_id}: {title}")
            
            # Logic nghỉ và restart driver (Giữ nguyên của TopCV)
            # (Sẽ không chạy nếu chỉ test 5 jobs, nhưng để đó không sao)
            if success_count % JOBS_PER_BREAK == 0 and success_count < len(new_jobs_to_crawl):
                pause_time = random.uniform(BREAK_DURATION_MIN, BREAK_DURATION_MAX)
                write_log(f"⏸ Đã cào {success_count} job. Tạm nghỉ {round(pause_time/60, 1)} phút...")
                time.sleep(pause_time)
            
            if idx % BATCH_SIZE == 0 and idx < len(new_jobs_to_crawl):
                write_log("🔄 Khởi động lại trình duyệt...")
                driver.quit()
                time.sleep(random.uniform(20, 40))
                driver = create_driver()

        except Exception as e:
            error_count += 1
            write_log(f"❌ Lỗi khi xử lý link {idx}/{len(new_jobs_to_crawl[:5])} (ID: {job_id}): {link} | {e}")

driver.quit()

# ==========================================================
# ===== KẾT THÚC VÀ CẬP NHẬT FILE ĐẾM TRANG =====
# ==========================================================

# =========================================================================
# ===== THAY ĐỔI 2: VÔ HIỆU HÓA CẬP NHẬT FILE ĐẾM TRANG =====
# Chúng ta không muốn lần chạy TEST này ảnh hưởng đến lần chạy THẬT
# =========================================================================
# new_max_page = max_pages_to_crawl_this_run + PAGES_TO_ADD_PER_RUN
# try:
#     with open(max_page_file, "w") as f:
#         f.write(str(new_max_page))
#     write_log(f"🔄 Đã cập nhật {max_page_file} cho lần chạy tiếp theo: {new_max_page}")
# except Exception as e:
#     write_log(f"❌ Không thể cập nhật file {max_page_file}: {e}")

write_log("--- CHẾ ĐỘ TEST: Đã bỏ qua bước cập nhật file đếm trang ---")

end_time = time.time()
total_minutes = round((end_time - start_time) / 60, 2)
write_log(f"🏁 (Test) Crawl xong trong {total_minutes} phút - Đã lưu {success_count} job MỚI, Lỗi: {error_count}")