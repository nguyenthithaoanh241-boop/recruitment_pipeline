import requests
import json
import time
from datetime import date
from bs4 import BeautifulSoup # Vẫn cần để dọn dẹp HTML từ API chi tiết

# --- 1. ĐỊNH NGHĨA CÁC API ---

# API 1: Lấy DANH SÁCH Job ID nổi bật (GET)
API_LIST_URL = "https://ms.vietnamworks.com/outstanding-jobs/v2/jobs/filter-outstanding"

# API 2: Lấy CHI TIẾT Job từ ID (POST)
API_DETAIL_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"

# --- 2. HÀM GỌI API CHI TIẾT (Hàm quan trọng nhất) ---

def get_job_details_api(job_id_can_cao):
    """
    Sử dụng API POST (job-search) để lấy chi tiết 1 job từ jobId.
    """
    
    # Headers chúng ta đã tìm thấy ở F12
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://www.vietnamworks.com",
        "Referer": "https://www.vietnamworks.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "X-Source": "Job-Details" 
    }
    
    # Payload yêu cầu chi tiết và lấy tất cả các trường
    payload = {
        "filter": [
            # Chuyển ID (dạng string) thành số nguyên nếu cần, 
            # nhưng API này có vẻ chấp nhận cả string
            {"field": "jobId", "value": job_id_can_cao} 
        ],
        "hitsPerPage": 1,
        "page": 0,
        "retrieveFields": ["*"]  # Yêu cầu trả về TẤT CẢ thông tin
    }

    try:
        # Gửi yêu cầu POST
        response = requests.post(API_DETAIL_URL, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json()
        
        # Bóc tách dữ liệu (lấy phần tử đầu tiên trong list 'data')
        if data.get('data') and len(data['data']) > 0:
            job_details = data['data'][0]
            return job_details # Trả về 1 dict đầy đủ
        else:
            print(f"   [Lỗi] API chi tiết trả về rỗng cho ID: {job_id_can_cao}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"   [Lỗi] Khi gọi API chi tiết cho ID {job_id_can_cao}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"   [Lỗi] Không thể parse JSON chi tiết cho ID {job_id_can_cao}")
        return None

# --- 3. HÀM CHẠY CHÍNH ---

def main():
    ngay_cao_du_lieu = date.today().strftime("%d/%m/%Y")
    all_full_data = [] # Nơi lưu trữ tất cả dữ liệu đầy đủ
    
    # --- BƯỚC 1: LẤY DANH SÁCH ID TỪ API (GET) ---
    try:
        response_list = requests.get(API_LIST_URL)
        response_list.raise_for_status()
        json_data_list = response_list.json()
        job_list_summary = json_data_list.get('data', [])
        
        if not job_list_summary:
            print("Lỗi: API danh sách không trả về tin nào.")
            return

    except Exception as e:
        print(f"Lỗi khi gọi API danh sách (Bước 1): {e}")
        return

    print(f"--- Bắt đầu cào {len(job_list_summary[:5])} tin (Quy trình 2-API) ---")
    print(f"Ngày cào: {ngay_cao_du_lieu}\n")

    # --- BƯỚC 2: LẶP QUA DANH SÁCH VÀ LẤY CHI TIẾT TỪNG TIN (POST) ---
    for i, job_summary in enumerate(job_list_summary[:5]):
        
        # Lấy ID từ API danh sách
        job_id = job_summary.get('id')
        if not job_id:
            print(f"--- Bỏ qua tin số {i+1} (Không có ID) ---")
            continue
        
        print(f"==================== Đang lấy tin SỐ {i+1} (ID: {job_id}) ====================")

        try:
            # GỌI API CHI TIẾT (Hàm chúng ta vừa định nghĩa)
            details = get_job_details_api(job_id)
            
            if not details:
                print(f"Không tìm thấy chi tiết cho ID: {job_id}\n")
                continue

            # --- Bóc tách dữ liệu ĐẦY ĐỦ từ API chi tiết (POST) ---
            tieu_de = details.get('jobTitle', 'N/A')
            cong_ty = details.get('companyName', 'N/A')
            luong = details.get('prettySalary', 'N/A')
            link_job = details.get('jobUrl', 'N/A')

            # Xử lý skills (là list of dicts, ví dụ: [{'skillName': 'SQL'},...])
            ky_nang_list = details.get('skills', [])
            ky_nang_sach = ", ".join([s.get('skillName', '') for s in ky_nang_list if s]) if ky_nang_list else "N/A"
            
            # Xử lý locations (là list of dicts)
            dia_diem_list = details.get('locations', [])
            dia_diem_sach = ", ".join([loc.get('locationNameVI', '') for loc in dia_diem_list if loc]) if dia_diem_list else "N/A"

            # --- DÙNG BEAUTIFULSOUP ĐỂ LÀM SẠCH HTML ---
            html_mo_ta = details.get('jobDescription', '')
            html_yeu_cau = details.get('jobRequirement', '')
            
            mo_ta_sach = BeautifulSoup(html_mo_ta, "html.parser").get_text(separator=" ", strip=True)
            yeu_cau_sach = BeautifulSoup(html_yeu_cau, "html.parser").get_text(separator=" ", strip=True)

            # --- In kết quả đầy đủ ---
            print(f"Tiêu đề: {tieu_de}")
            print(f"Công ty: {cong_ty}")
            print(f"Lương: {luong}")
            print(f"Địa điểm: {dia_diem_sach}")
            print(f"Kỹ năng: {ky_nang_sach}")
            print(f"Link: {link_job}")
            
            print("\n--- Mô tả công việc (Đầy đủ) ---")
            print(mo_ta_sach if mo_ta_sach else "(Không có mô tả)")
            
            print("\n--- Yêu cầu công việc (Đầy đủ) ---")
            print(yeu_cau_sach if yeu_cau_sach else "(Không có yêu cầu)")
            print(f"===========================================================\n")

            # Thêm độ trễ 1 giây để tránh bị block IP
            time.sleep(1) 

        except Exception as e:
            print(f"Lỗi khi xử lý ID {job_id}: {e}\n")

    print("--- Hoàn thành ---")


# --- CHẠY CHƯƠNG TRÌNH ---
if __name__ == "__main__":
    
    main()