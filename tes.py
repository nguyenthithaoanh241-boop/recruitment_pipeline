import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- 1. Cài đặt và Khởi động Trình duyệt ---
print("Đang khởi động trình duyệt...")
try:
    # Tự động cài đặt và quản lý ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    driver.maximize_window() # Mở toàn màn hình cho dễ nhìn
except Exception as e:
    print(f"Lỗi khởi động trình duyệt: {e}")
    print("Vui lòng kiểm tra lại trình duyệt Chrome và kết nối mạng.")
    exit() # Thoát nếu không mở được trình duyệt

# URL mục tiêu 
url = "https://careerviet.vn/viec-lam/cntt-phan-mem-c1-vi.html"

# --- 2. Truy cập Trang web ---
print(f"Đang truy cập: {url}")
driver.get(url)

# --- 3. Đợi Trang tải và Tìm kiếm Job Cards ---
CARD_SELECTOR = ".job-item.has-background" 

# "a.job_title" là thẻ <a> (link) có class "job_title" NẰM BÊN TRONG card
LINK_SELECTOR = "div.title" 

job_links_list = []

try:
    # 3.1. Đợi tối đa 20 giây cho đến khi ÍT NHẤT MỘT job card xuất hiện
    # Đây là bước quan trọng để tránh lỗi "timeout" hoặc "không tìm thấy"
    print("Đang đợi trang tải xong danh sách việc làm...")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SELECTOR))
    )
    print("Trang đã tải xong!")
    
    # 3.2. Tìm TẤT CẢ các phần tử khớp với CARD_SELECTOR
    all_job_cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)

    # 3.3. Đếm tổng số
    total_jobs = len(all_job_cards)
    print("--- KẾT QUẢ BƯỚC 1 ---")
    print(f"Tìm thấy tổng cộng: {total_jobs} job cards trên trang này.")

    # 3.4. Lặp qua 5 card đầu tiên để lấy link
    print("\nIn 5 link công việc đầu tiên:")
    
    # Dùng slicing [ :5 ] để chỉ lấy 5 phần tử đầu tiên
    for card in all_job_cards[:2]:
        try:
            # Tìm link bên TRONG từng card
            link_element = card.find_element(By.CSS_SELECTOR, LINK_SELECTOR)
            
            # Lấy thuộc tính 'href' (đây chính là đường link)
            job_link = link_element.get_attribute('href')
            print(job_link)
            job_links_list.append(job_link) # Lưu lại để dùng sau
            
        except Exception as e:
            # Nếu có 1 card bị lỗi (ví dụ: card quảng cáo) thì bỏ qua
            print(f"Lỗi khi lấy link từ một card: {e}")

except Exception as e:
    print(f"Đã xảy ra lỗi nghiêm trọng: {e}")

finally:
    # --- 4. Đóng Trình duyệt ---
    print("\nĐã hoàn thành Bước 1.")
    print("Trình duyệt sẽ tự động đóng sau 5 giây.")
    time.sleep(5)
    driver.quit() # Luôn luôn đóng trình duyệt sau khi xong