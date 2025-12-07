import sys
import os
import pandas as pd
import numpy as np
import re
import sqlalchemy
import hashlib
from datetime import datetime, timedelta

# ==============================================================================
# 1. CẤU HÌNH & IMPORT TỪ CONFIG
# ==============================================================================
# Thiết lập đường dẫn để import được 'pipeline.config'
current_dir = os.getcwd()
# Giả sử cấu trúc thư mục: Project_Root/pipeline/config.py và Project_Root/notebooks/etl_script.py
# Ta cần thêm Project_Root vào sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..')) 
if project_root not in sys.path:
    sys.path.append(project_root)

# Thử import DATABASE_URL từ file config của bạn
try:
    from pipeline.config import DATABASE_URL
    print("✅ Đã lấy Connection String từ pipeline/config.py thành công!")
except ImportError:
    # Fallback xử lý nếu chạy trực tiếp tại root mà không tìm thấy module
    sys.path.append(current_dir)
    try:
        from pipeline.config import DATABASE_URL
        print("✅ Đã lấy Connection String từ pipeline/config.py (tại root) thành công!")
    except ImportError as e:
        print(f"Lỗi import config: {e}")
        print("Vui lòng kiểm tra lại đường dẫn file config.py.")
        DATABASE_URL = None
# ==============================================================================
# 2. CLASS ETL (LOGIC 63 TỈNH -> 34 ĐẦU MỐI + TỌA ĐỘ CHI TIẾT)
# ==============================================================================
class RecruitmentETL:
    def __init__(self, connection_string):
        self.engine = sqlalchemy.create_engine(connection_string)
        print("✅ Đã khởi tạo cấu hình & Logic Geo Mapping (Chi tiết -> Gộp).")
        
        # 1. Bảng luật gộp (63 -> 34)
        self.merge_map = self._init_merge_mapping()
        # 2. Bảng tọa độ chi tiết (Full 63 tỉnh để lấy tọa độ gốc)
        self.coord_map = self._init_full_coords()
        
        # Các từ điển khác
        self.industry_map = self._init_industry_map()
        self.job_title_map = self._init_job_title_map()
        self.skill_map = self._init_skill_map()

    # --------------------------------------------------------------------------
    # A. TỪ ĐIỂN LUẬT GỘP (INPUT -> TỈNH ĐÍCH)
    # --------------------------------------------------------------------------
    def _find_experience_coalesced(self, row):
        """
        Ưu tiên lấy từ cột YeuCauKinhNghiem, nếu trống thì tìm kiếm trong YeuCauUngVien.
        """
        # 1. Kiểm tra cột chính (YeuCauKinhNghiem)
        raw_exp_primary = row.get('YeuCauKinhNghiem')
        min_val, max_val = self._extract_experience_numerics(raw_exp_primary)

        # 2. Coalesce/Fallback: Nếu cột chính không tìm thấy số nào (trả về None, None)
        if pd.isna(min_val) and pd.isna(max_val):
            # Thử tìm kiếm trong cột phụ (YeuCauUngVien)
            raw_exp_secondary = row.get('YeuCauUngVien')
            min_val, max_val = self._extract_experience_numerics(raw_exp_secondary)

        # 3. Đảm bảo trả về NaN nếu không tìm thấy gì (để logic Imputation sau xử lý)
        if pd.isna(min_val) and pd.isna(max_val):
            return pd.Series([np.nan, np.nan]) 

        return pd.Series([min_val, max_val])
    def _init_merge_mapping(self):
        """Từ điển: Tên tìm thấy trong JD -> Tên Tỉnh Gộp (34 Tỉnh)"""
        return {
            # --- Nhóm 1: Miền Tây & Nam Bộ ---
            "kiên giang": "An Giang", "an giang": "An Giang",
            "bạc liêu": "Cà Mau", "cà mau": "Cà Mau",
            "bình phước": "Đồng Nai", "đồng nai": "Đồng Nai",
            "tiền giang": "Đồng Tháp", "đồng tháp": "Đồng Tháp",
            "long an": "Tây Ninh", "tây ninh": "Tây Ninh",
            "bến tre": "Vĩnh Long", "trà vinh": "Vĩnh Long", "vĩnh long": "Vĩnh Long",
            "sóc trăng": "TP. Cần Thơ", "hậu giang": "TP. Cần Thơ", "cần thơ": "TP. Cần Thơ", 
            "tp. cần thơ": "TP. Cần Thơ", "tp cần thơ": "TP. Cần Thơ",

            # --- Nhóm 2: Miền Trung & Tây Nguyên ---
            "phú yên": "Đắk Lắk", "đắk lắk": "Đắk Lắk", "dak lak": "Đắk Lắk",
            "bình định": "Gia Lai", "gia lai": "Gia Lai",
            "ninh thuận": "Khánh Hoà", "khánh hoà": "Khánh Hoà", "khánh hòa": "Khánh Hoà", "nha trang": "Khánh Hoà",
            "đắk nông": "Lâm Đồng", "dak nong": "Lâm Đồng", "bình thuận": "Lâm Đồng", "lâm đồng": "Lâm Đồng", "đà lạt": "Lâm Đồng",
            "kon tum": "Quảng Ngãi", "quảng ngãi": "Quảng Ngãi",
            "quảng bình": "Quảng Trị", "quảng trị": "Quảng Trị",
            "quảng nam": "TP. Đà Nẵng", "đà nẵng": "TP. Đà Nẵng", "tp. đà nẵng": "TP. Đà Nẵng", "tp đà nẵng": "TP. Đà Nẵng",
            "thừa thiên huế": "TP. Huế", "huế": "TP. Huế", "tp. huế": "TP. Huế", "tp huế": "TP. Huế",

            # --- Nhóm 3: Miền Bắc ---
            "bắc giang": "Bắc Ninh", "bắc ninh": "Bắc Ninh",
            "thái bình": "Hưng Yên", "hưng yên": "Hưng Yên",
            "yên bái": "Lào Cai", "lào cai": "Lào Cai",
            "hà nam": "Ninh Bình", "nam định": "Ninh Bình", "ninh bình": "Ninh Bình",
            "hòa bình": "Phú Thọ", "vĩnh phúc": "Phú Thọ", "phú thọ": "Phú Thọ",
            "bắc kạn": "Thái Nguyên", "bắc cạn": "Thái Nguyên", "thái nguyên": "Thái Nguyên",
            "hà giang": "Tuyên Quang", "tuyên quang": "Tuyên Quang",
            "hải dương": "TP. Hải Phòng", "hải phòng": "TP. Hải Phòng", "tp. hải phòng": "TP. Hải Phòng",
            "hà nội": "TP. Hà Nội", "hn": "TP. Hà Nội", "tp. hà nội": "TP. Hà Nội", "tp hà nội": "TP. Hà Nội",

            # --- Nhóm 4: TP. HCM ---
            "bình dương": "TP. Hồ Chí Minh", "bà rịa": "TP. Hồ Chí Minh", "vũng tàu": "TP. Hồ Chí Minh",
            "bà rịa - vũng tàu": "TP. Hồ Chí Minh", "hồ chí minh": "TP. Hồ Chí Minh", "hcm": "TP. Hồ Chí Minh",
            "tphcm": "TP. Hồ Chí Minh", "sg": "TP. Hồ Chí Minh", "sài gòn": "TP. Hồ Chí Minh", "tp. hồ chí minh": "TP. Hồ Chí Minh",

            # --- Nhóm 5: Các tỉnh giữ nguyên ---
            "cao bằng": "Cao Bằng", "điện biên": "Điện Biên", "hà tĩnh": "Hà Tĩnh",
            "lai châu": "Lai Châu", "lạng sơn": "Lạng Sơn", "nghệ an": "Nghệ An",
            "quảng ninh": "Quảng Ninh", "sơn la": "Sơn La", "thanh hóa": "Thanh Hóa"
        }
    def _init_full_coords(self):
        """Từ điển tọa độ GỐC (Full 63 Tỉnh)"""
        # Format: "key": ("Khu Vực", Lat, Long, "Tên Gốc Hiển Thị")
        return {
            "hà nội": ("Bắc", 21.0285, 105.8542, "TP. Hà Nội"), "hn": ("Bắc", 21.0285, 105.8542, "TP. Hà Nội"),
            "bắc giang": ("Bắc", 21.2731, 106.1946, "Bắc Giang"), # Tọa độ riêng
            "bắc ninh": ("Bắc", 21.1861, 106.0763, "Bắc Ninh"),
            "hải dương": ("Bắc", 20.9409, 106.3330, "Hải Dương"), 
            "hưng yên": ("Bắc", 20.9333, 106.3167, "Hưng Yên"),
            "hải phòng": ("Bắc", 20.8449, 106.6881, "TP. Hải Phòng"),
            "vĩnh phúc": ("Bắc", 21.3093, 105.6053, "Vĩnh Phúc"), 
            "thái nguyên": ("Bắc", 21.5672, 105.8244, "Thái Nguyên"),
            "thái bình": ("Bắc", 20.4475, 106.3364, "Thái Bình"),
            "nam định": ("Bắc", 20.4200, 106.1683, "Nam Định"), 
            "ninh bình": ("Bắc", 20.2541, 105.9751, "Ninh Bình"),
            "hà nam": ("Bắc", 20.5453, 105.9122, "Hà Nam"),
            "phú thọ": ("Bắc", 21.3220, 105.2280, "Phú Thọ"), 
            "hòa bình": ("Bắc", 20.8172, 105.3377, "Hòa Bình"),
            "bắc kạn": ("Bắc", 22.1472, 105.8364, "Bắc Kạn"),
            "tuyên quang": ("Bắc", 21.8251, 105.2155, "Tuyên Quang"),
            "lào cai": ("Bắc", 22.4851, 103.9707, "Lào Cai"), 
            "yên bái": ("Bắc", 21.7229, 104.9113, "Yên Bái"),
            "lạng sơn": ("Bắc", 21.8538, 106.7607, "Lạng Sơn"), 
            "cao bằng": ("Bắc", 22.6667, 106.2500, "Cao Bằng"),
            "hà giang": ("Bắc", 22.8233, 104.9839, "Hà Giang"), 
            "sơn la": ("Bắc", 21.3283, 103.9015, "Sơn La"),
            "lai châu": ("Bắc", 22.4014, 103.2736, "Lai Châu"), 
            "điện biên": ("Bắc", 21.3850, 103.0210, "Điện Biên"),
            "quảng ninh": ("Bắc", 20.9500, 107.0833, "Quảng Ninh"),

            # Miền Trung
            "thanh hóa": ("Trung", 19.8077, 105.7765, "Thanh Hóa"), "nghệ an": ("Trung", 18.6734, 105.6791, "Nghệ An"),
            "hà tĩnh": ("Trung", 18.3427, 105.9058, "Hà Tĩnh"), "quảng bình": ("Trung", 17.4833, 106.6000, "Quảng Bình"),
            "quảng trị": ("Trung", 16.7423, 107.1856, "Quảng Trị"), "huế": ("Trung", 16.4637, 107.5909, "TP. Huế"),
            "đà nẵng": ("Trung", 16.0544, 108.2022, "TP. Đà Nẵng"), "quảng nam": ("Trung", 15.5804, 108.4816, "Quảng Nam"),
            "quảng ngãi": ("Trung", 15.1205, 108.7923, "Quảng Ngãi"), "bình định": ("Trung", 13.7830, 109.2197, "Bình Định"),
            "phú yên": ("Trung", 13.0882, 109.0913, "Phú Yên"), "khánh hòa": ("Trung", 12.2388, 109.1967, "Khánh Hoà"),
            "ninh thuận": ("Trung", 11.5647, 108.9902, "Ninh Thuận"), "bình thuận": ("Trung", 10.9333, 108.1000, "Bình Thuận"),
            "kon tum": ("Trung", 14.3500, 108.0000, "Kon Tum"), "gia lai": ("Trung", 13.9833, 108.0000, "Gia Lai"),
            "đắk lắk": ("Trung", 12.6667, 108.0500, "Đắk Lắk"), "đắk nông": ("Trung", 12.0000, 107.6833, "Đắk Nông"),
            "lâm đồng": ("Trung", 11.9404, 108.4583, "Lâm Đồng"),

            # Miền Nam
            "hcm": ("Nam", 10.8231, 106.6297, "TP. Hồ Chí Minh"), "hồ chí minh": ("Nam", 10.8231, 106.6297, "TP. Hồ Chí Minh"),
            "bình dương": ("Nam", 10.9805, 106.6576, "Bình Dương"), "đồng nai": ("Nam", 10.9574, 106.8427, "Đồng Nai"),
            "bà rịa": ("Nam", 10.3460, 107.0843, "Bà Rịa - Vũng Tàu"), "vũng tàu": ("Nam", 10.3460, 107.0843, "Bà Rịa - Vũng Tàu"),
            "tây ninh": ("Nam", 11.3667, 106.1167, "Tây Ninh"), "bình phước": ("Nam", 11.5333, 106.9000, "Bình Phước"),
            "long an": ("Nam", 10.5333, 106.4000, "Long An"), "tiền giang": ("Nam", 10.3592, 106.3653, "Tiền Giang"),
            "bến tre": ("Nam", 10.2373, 106.3752, "Bến Tre"), "trà vinh": ("Nam", 9.9372, 106.3421, "Trà Vinh"),
            "vĩnh long": ("Nam", 10.2541, 105.9723, "Vĩnh Long"), "đồng tháp": ("Nam", 10.4564, 105.6425, "Đồng Tháp"),
            "an giang": ("Nam", 10.3759, 105.4185, "An Giang"), "cần thơ": ("Nam", 10.0452, 105.7469, "TP. Cần Thơ"),
            "hậu giang": ("Nam", 9.7842, 105.4700, "Hậu Giang"), "sóc trăng": ("Nam", 9.6033, 105.9722, "Sóc Trăng"),
            "kiên giang": ("Nam", 10.0076, 105.0869, "Kiên Giang"), "bạc liêu": ("Nam", 9.2922, 105.7249, "Bạc Liêu"),
            "cà mau": ("Nam", 9.1755, 105.1522, "Cà Mau")
        }
    def _init_industry_map(self):
        return {
            "Tài chính - Ngân hàng": ["đầu tư","kế toán", "kiểm toán", "thuế","ngân hàng", "chứng khoán", "tài chính", "bảo hiểm", "audit"],
            "Sản xuất & Kỹ thuật": [ "sản xuất", "vận hành sản xuất", "cơ khí", "ô tô", "tự động hóa", 
        "điện / điện tử", "điện lạnh", "điện công nghiệp", "bảo trì", "sửa chữa",
        "dệt may", "da giày", "thời trang", "gỗ", "nội thất", 
        "dầu khí", "khoáng sản", "năng lượng", "hóa học", "công nghiệp",
        "nông nghiệp", "nông lâm ngư nghiệp", "kỹ thuật ứng dụng", "quản lý chất lượng", "qa/qc", "khu công nghiệp"],
            "Thương mại điện tử & Bán lẻ": ["bán lẻ", "bán sỉ", "hàng tiêu dùng", "fmcg", "thực phẩm", "đồ uống", 
        "hàng gia dụng", "chăm sóc cá nhân", "thương mại tổng hợp", "siêu thị",
        "thương mại điện tử", "e-commerce","retail"],
            "Y tế & Sức khỏe": ["y tế", "dược", "bệnh viện", "chăm sóc sức khỏe", "thẩm mỹ", "làm đẹp", 
        "công nghệ sinh học", "hóa mỹ phẩm", "nha khoa", "healthcare", "pharma"],
            "Xây dựng & Bất động sản": ["real estate","xây dựng", "bất động sản", "kiến trúc", "thiết kế nội thất", "vật liệu xây dựng"],
                "Vận tải & Logistics": [
        "vận chuyển", "giao nhận", "kho vận", "logistics", "kho bãi", "hàng không", 
        "xuất nhập khẩu", "thu mua", "vật tư", "chuỗi cung ứng"
    ],

    
    "Dịch vụ & Giải trí": [
        "du lịch", "nhà hàng", "khách sạn", "nghệ thuật", "thiết kế", "giải trí", 
        "truyền hình", "báo chí", "biên tập", "xuất bản", "in ấn", "tổ chức sự kiện"
    ],

    
    "Giáo dục & Đào tạo": [
        "giáo dục", "đào tạo", "thư viện", "trường học", "trung tâm anh ngữ"
    ],

    
    "Marketing & Truyền thông": [
        "marketing", "tiếp thị", "quảng cáo", "truyền thông", "đối ngoại", 
        "pr", "agency", "digital marketing"
    ],

    
    "Dịch vụ doanh nghiệp": [
        "nhân sự", "hành chính", "thư ký", "luật", "pháp lý", 
        "biên phiên dịch", "thông dịch", "tư vấn", "dịch vụ khách hàng"
    ],
    
    
    "Công nghệ & Viễn thông": [
        "cntt", "phần mềm", "phần cứng", "mạng", "viễn thông", "bưu chính viễn thông",
        "internet", "online", "game", "it - phần mềm", "it - phần cứng"
    ],
    
    
    "Kinh doanh / Sales": [
        "bán hàng", "kinh doanh", "sales", "phát triển thị trường"
    ]

        }

    def _init_job_title_map(self):
        return {
    # --- GROUP: MANAGEMENT & CONSULTING ---
    "Project Manager": [
        "project manager", "pm ", "pm)", 
        "quản lý dự án", "pmo", "điều phối dự án", "quản trị dự án"
    
    ],
    "Project Leader": [
        "project leader", "trưởng dự án",
    
    ],
    "IT Manager": [
        "it manager", "trưởng phòng it", "giám đốc công nghệ", "cto", "cio", 
        "trưởng bộ phận it", "trưởng phòng công nghệ", "it section manager"
    ],
    "Tech Lead": [
        "tech lead", "technical lead", "trưởng nhóm kỹ thuật", "team lead"
    ],
    "IT Consultant": [
        "it consultant", "tư vấn giải pháp", "tư vấn công nghệ", "tư vấn kỹ thuật", 
        "triển khai phần mềm", "technical consultant", "giải pháp phần mềm"
    ],

    # --- GROUP: PRODUCT MANAGEMENT ---
    "Product Owner": [
        "product owner",  "po ", "quản lý sản phẩm", "giám đốc sản phẩm",
        "product executive", "phát triển sản phẩm"
    ],
    "Product Manager": [
        "product manager", "quản lý sản phẩm", "giám đốc sản phẩm",
    ],
    "Product Executive": [
        "product executive", "phát triển sản phẩm"
    ],
    "Business Analyst": [
        "business analyst", "ba ", "ba)", "phân tích nghiệp vụ", "phân tích kinh doanh",
        "business data analyst"
    ],

    # --- GROUP: SOFTWARE DEVELOPMENT ---
    "Full-stack Developer": [
        "fullstack", "full-stack", "full stack"
    ],
    "Back-end Developer": [
        "backend", "back-end", "back end","python developer"
    ],
    "Front-end Developer": [
        "frontend", "front-end", "front end", "web developer",
        
    ],
    "Mobile Developer": [
        "mobile", "android", "ios", "flutter", "react native", "swift", "kotlin", "xamarin"
    ],
    "Game Developer": [
        "game developer", "unity"
    ],
    "Embedded Engineer": [
        "embedded", "nhúng", "firmware", "iot", "vi mạch", "lập trình máy cnc", "plc", "scada"
    ],

    # --- GROUP: TESTING ---
    "Tester": [
        "tester", "kiểm thử", "manual test", "test engineer"
    ],
    "QA - QC": [
        "qa", "qc", "quality assurance", "quality control", "pqa", "đảm bảo chất lượng"
    ],

    # --- GROUP: CLOUD & INFRASTRUCTURE ---
    "DevOps Engineer/DevSecOps Engineer": [
        "devops", "sre", "devsecops", "ci/cd", "site reliability"
    ],
    "Cloud Engineer": [
        "cloud engineer", "aws", "azure", "gcp", "kỹ sư đám mây"
    ],
    "System Engineer": [
        "system engineer",
        "kỹ sư mạng", "an ninh mạng", "security", "bảo mật", "hạ tầng"
    ],
     "System Admin": [
        "system admin", "sysadmin", "quản trị hệ thống"
    ],

    # --- GROUP: DATA ANALYTICS ---
    "Data Engineer": [
        "data engineer", "kỹ sư dữ liệu", "big data", "etl","kĩ sư dữ liệu"
    ],
    "Data Analyst": [
        "data analyst","phân tích dữ liệu","chuyên viên phân tích dữ liệu"
    ],
    "Data Scientist": [
        "data scientist", "khoa học dữ liệu"
    ],
    "Business Intelligence Analyst": [
        "business intelligence", "bi analyst", "bi executive"
    ],
    "Database Engineer": [
        "dba", "database", "cơ sở dữ liệu", "sql developer", "quản trị csdl"
    ],

    # --- GROUP: AI & BLOCKCHAIN ---
    "AI Engineer": [
        "ai engineer", "trí tuệ nhân tạo", "machine learning"
    ],
    "Blockchain Engineer": [
        "blockchain", 
        "computer vision", "nlp", "smart contract", "web3", "solidity"
    ],

    # --- GROUP: DESIGNING ---
    "Designer": [
        "designer", "thiết kế", "ui/ux", "graphic", "đồ họa", "art director", "artist"
    ],

    # --- GROUP: KHÁC (Các mục còn lại trong ảnh) ---
    "ERP Engineer/ERP Consultant": [
        "erp", "sap", "odo", "salesforce", "crm developer"
    ],
    "Solution Architect": [
        "solution architect", "kiến trúc sư", "kiến trúc hệ thống"
    ],
    "IT Support": [
        "it support", "helpdesk", "hỗ trợ kỹ thuật", "kỹ thuật máy tính", "sửa chữa", 
        "lắp ráp", "kỹ thuật viên", "it phần cứng", "it helpdesk"
    ]
}
        
    def _init_skill_map(self):
        return {
            "hard": {
        # --- Ngôn ngữ lập trình ---
        "Python": ["python"],
        "Java": ["java ", "java,"], 
        "Go/Golang": ["golang", "go lang"], 
        "JavaScript": ["javascript", "js ", "js,", "js."],
        "TypeScript": ["typescript", "ts"],
        "C++": ["c\+\+"], 
        "C#": ["c#", ".net", "dotnet"],
        "PHP": ["php"],
        "Ruby": ["ruby", "rails"],
        "Swift": ["swift"],
        "Kotlin": ["kotlin"],
        "Dart": ["dart", "flutter"], 
        "R": ["r lang", "r programming"], 
        "SQL": ["sql", "mysql", "postgres", "sql server", "nosql", "mongodb", "redis"], # Gộp DB vào đây
        "HTML/CSS": ["html", "css"],
        "Rust": ["rust"],
        "Scala": ["scala"],
        "Bash/Shell": ["bash", "shell script", "linux"],
        "PowerShell": ["powershell"],
        "VBA": ["vba", "excel macro"],
        "MATLAB": ["matlab"],
        "Assembly": ["assembly", "asm"],
        
        # --- Framework/Lib ---
        "React": ["react", "reactjs", "react.js", "react native"],
        "Angular": ["angular"],
        "Vue": ["vue", "vuejs"],
        "NodeJS": ["node", "nodejs", "node.js"],
        "Spring": ["spring boot", "spring mvc"],
        "Django/Flask": ["django", "flask"],
        
        # --- Cloud & DevOps (Đã gộp trùng) ---
        "AWS": ["aws", "amazon web services"],
        "Azure": ["azure"],
        "GCP": ["gcp", "google cloud"],
        "Docker": ["docker"],
        "Kubernetes": ["k8s", "kubernetes"],
        "Git": ["git", "github", "gitlab", "svn"],
        
        # --- Data Visualization & Analytics ---
        "Excel": ["excel", "spreadsheet", "google sheet", "google sheets", "vlookup", "pivot table"],
        "Power BI": ["power bi", "powerbi", "dax", "power query"],
        "Tableau": ["tableau"],
        "Looker": ["looker", "google data studio"],
        "Qlik": ["qlik", "qlikview", "qliksense"],
        "SAS/SPSS": ["sas", "spss"],
        
        # --- Công cụ Quản lý & Design ---
        "Jira/Confluence": ["jira", "confluence", "atlassian"],
        "Trello/Asana": ["trello", "asana", "monday.com"],
        "Office/Tin học": ["word", "powerpoint", "ms office", "tin học văn phòng"],
        "Design Tool": ["figma", "photoshop", "adobe xd", "sketch"]
    }
,
            "soft": {
        # --- Giao tiếp & Lãnh đạo ---
        "Giao tiếp": ["giao tiếp", "communication", "trình bày", "thuyết trình", "presentation"],
        "Lãnh đạo": ["lãnh đạo", "leadership", "dẫn dắt", "quản lý nhóm", "team lead"],
        "Thương lượng": ["thương lượng", "đàm phán", "negotiation"],
        
        # --- Tư duy ---
        "Giải quyết vấn đề": ["giải quyết vấn đề", "problem solving", "xử lý tình huống"],
        "Tư duy phản biện": ["phản biện", "critical thinking", "tư duy logic"],
        "Sáng tạo": ["sáng tạo", "creative", "innovation"],
        
        # --- Thái độ ---
        "Quản lý thời gian": ["quản lý thời gian", "time management", "sắp xếp công việc"],
        "Làm việc nhóm": ["làm việc nhóm", "teamwork", "team work", "hòa đồng"],
        "Chịu áp lực": ["chịu được áp lực", "work under pressure", "áp lực cao"],
        "Tự học": ["tự học", "self-learning", "thích nghi", "ham học hỏi"],
        
        # --- Ngoại ngữ ---
        "Tiếng Anh": ["tiếng anh", "english", "toeic", "ielts", "toefl"],
        "Tiếng Nhật": ["tiếng nhật", "japanese", "n1", "n2", "n3"],
        "Tiếng Trung": ["tiếng trung", "chinese", "hsk"],
        "Tiếng Hàn": ["tiếng hàn", "korean", "topik"]
    }
        }

    # ==========================================================================
    # C. CÁC HÀM XỬ LÝ (TRANSFORMATION)
    # ==========================================================================
    
    # 1. Hàm quan trọng nhất: Xử lý Địa điểm (Gộp Tỉnh + Giữ tọa độ gốc)
    def clean_location_data(self, df):
        # 1. TÁCH DÒNG (Explode)
        # Tách địa điểm nếu có dấu phân cách (, ; | & hoặc - )
        df['Temp_Loc_List'] = df['ViTri'].astype(str).apply(
            lambda x: [i.strip() for i in re.split(r'[;,|&]|\s+-\s+', x) if i.strip()]
        )
        # Bùng nổ dòng
        df_exploded = df.explode('Temp_Loc_List')
        
        # 2. HÀM MAPPING TỪNG DÒNG
        def get_geo_info(loc_raw):
            loc_check = str(loc_raw).lower().strip()
            
            # --- [A] TÌM TỈNH ĐÍCH (Gộp 34 Tỉnh) ---
            tinh_gop = "Khác"
            # Kiểm tra chính xác
            if loc_check in self.merge_map:
                tinh_gop = self.merge_map[loc_check]
            else:
                # Kiểm tra chứa trong (vd: "Tại Bắc Giang" -> Bắc Ninh)
                for k, v in self.merge_map.items():
                    if k in loc_check:
                        tinh_gop = v
                        break
                if tinh_gop == "Khác":
                    tinh_gop = loc_raw # Nếu không thuộc luật gộp thì giữ nguyên
            
            # --- [B] TÌM TỌA ĐỘ GỐC (Khu vực của chính nó) ---
            region, lat, long, original_name = "Khác", None, None, loc_raw
            
            # Kiểm tra chính xác trong Dict tọa độ
            if loc_check in self.coord_map:
                info = self.coord_map[loc_check]
                region, lat, long, original_name = info[0], info[1], info[2], info[3]
            else:
                # Kiểm tra chứa trong
                for k, info in self.coord_map.items():
                    if k in loc_check:
                        region, lat, long, original_name = info[0], info[1], info[2], info[3]
                        break
            
            # Trả về: [Tên gốc sạch, Tỉnh đã gộp, Khu vực, Lat, Long]
            return pd.Series([original_name, tinh_gop, region, lat, long])

        # Áp dụng
        df_exploded[['ViTri_clean', 'Tinh_Thanh', 'KhuVuc', 'Latitute', 'longtitute']] = df_exploded['Temp_Loc_List'].apply(get_geo_info)
        return df_exploded

    # Các hàm clean khác (Giữ nguyên logic)
    def clean_title(self, text):
        if not isinstance(text, str): return "Khác"
        text = text.lower()
        for pat in [r'tuyển', r'gấp', r'hcm', r'hn', r'fulltime', r'remote']: text = re.sub(pat, '', text)
        for std, kws in self.job_title_map.items():
            if any(k in text for k in kws): return std
        return "Khác"

    def clean_salary(self, text):
        if not isinstance(text, str): return pd.Series([0.0, 0.0])
        text = text.lower().replace(',', '').replace('.', '')
        unit = 25000 if 'usd' or '$' in text else (1000000 if any(x in text for x in ['triệu', 'tr', 'm']) else 1)
        nums = [float(x) for x in re.findall(r'\d+', text)]
        mi, ma = 0.0, 0.0
        if len(nums) == 1: mi, ma = (nums[0]*unit, nums[0]*unit)
        elif len(nums) >= 2: mi, ma = (nums[0]*unit, nums[1]*unit)
        return pd.Series([mi, ma])
    def clean_deadline(self, row):
        try:
            # Lấy chuỗi gốc và đưa về chữ thường
            raw = str(row.get('HanNopHoSo', '')).lower().strip()
            # Lấy ngày cào dữ liệu làm mốc (nếu null thì lấy hôm nay)
            ref_date = pd.to_datetime(row.get('NgayCaoDuLieu', datetime.now()))
            
            # Case 1: Bắt định dạng "Hạn nộp hồ sơ: 30/04/2025"
            if "hạn nộp" in raw:
                m = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', raw)
                if m: 
                    return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).date()
            
            # Case 2: Bắt định dạng "Còn 20 ngày tới"
            if "ngày tới" in raw:
                m = re.search(r'(\d+)', raw)
                if m: 
                    return (ref_date + timedelta(days=int(m.group(1)))).date()
            
            # Case 3: Thử parse trực tiếp (VD: "2025-04-30")
            parsed = pd.to_datetime(raw, dayfirst=True, errors='coerce')
            return parsed.date() if not pd.isna(parsed) else None
            
        except: 
            return None

    def clean_experience(self, text):
        text = str(text).lower()
        if 'không' in text: return pd.Series([0.0, 0.0, 0.0])
        nums = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', text)]
        if not nums: return pd.Series([None, None, None])
        mi, ma = (nums[0], nums[0]) if len(nums)==1 else (min(nums), max(nums))
        return pd.Series([mi, (mi+ma)/2, ma])
    # --- HÀM PHỤ TRỢ 1: TRÍCH XUẤT SỐ LIỆU LƯƠNG (CHI TIẾT) ---
    def _extract_salary_numerics(self, raw_salary):
        if not isinstance(raw_salary, str) or not raw_salary: 
            return pd.Series([0.0, 0.0])
        
        text = raw_salary.lower()
        unit = 1
        # Xử lý đơn vị tiền tệ & Tỷ giá
        if "usd" in text or "$" in text: unit = 25000
        elif any(x in text for x in ["triệu", "tr", "millions", "m"]): unit = 1000000
        elif any(x in text for x in ["nghìn", "k"]): unit = 1000
            
        text_clean = text.replace(',', '')
        matches = re.findall(r'\d+(?:\.\d+)?', text_clean)
        nums = [float(n) for n in matches]
        
        if not nums: return pd.Series([0.0, 0.0])
        
        min_sal, max_sal = 0.0, 0.0
        if len(nums) == 1:
            val = nums[0] * unit
            # Xử lý các từ khóa biên (Up to, Min, Max...)
            if any(kw in text for kw in ["đến", "tới", "up to", "dưới", "max"]): min_sal, max_sal = 0, val
            elif any(kw in text for kw in ["từ", "trên", "hơn", "min"]): min_sal, max_sal = val, 0
            else: min_sal = max_sal = val
        elif len(nums) >= 2:
            min_sal, max_sal = nums[0] * unit, nums[1] * unit
            
        return pd.Series([min_sal, max_sal])
    # --------------------------------------------------------------------------
    # LOGIC KINH NGHIỆM (Chính xác theo code bạn cung cấp)
    # --------------------------------------------------------------------------
    def _extract_experience_numerics(self, raw_exp):
        if not isinstance(raw_exp, str) or not raw_exp:
            return pd.Series([None, None])

        text = raw_exp.lower().strip()
        
        # 1. Check từ khóa "Không kinh nghiệm"
        no_exp_keywords = ['không yêu cầu', 'chưa có kinh nghiệm', 'không cần kinh nghiệm', 'no experience']
        if any(kw in text for kw in no_exp_keywords):
            return pd.Series([0.0, 0.0])

        # 2. Parse số
        matches = re.findall(r'\d+(?:\.\d+)?', text)
        nums = [float(n) for n in matches]

        if not nums: return pd.Series([None, None])

        # 3. Quy đổi Tháng -> Năm
        if 'tháng' in text and 'năm' not in text:
            nums = [n / 12 for n in nums]

        min_exp, max_exp = None, None

        # 4. Logic phân tích khoảng
        if 'dưới 1 năm' in text:
            min_exp, max_exp = 0.0, 1.0
        elif any(kw in text for kw in ['trên', 'hơn', 'over', '>']):
            min_exp = nums[0]
            max_exp = None 
        elif any(kw in text for kw in ['dưới', 'less than', '<']):
            min_exp = 0.0
            max_exp = nums[0]
        elif len(nums) >= 2:
            nums.sort()
            min_exp, max_exp = nums[0], nums[-1]
        elif len(nums) == 1:
            min_exp, max_exp = nums[0], nums[0]

        return pd.Series([min_exp, max_exp])

    # --------------------------------------------------------------------------
    # LOGIC QUY MÔ (Chính xác theo code bạn cung cấp)
    # --------------------------------------------------------------------------
    def _extract_size_numerics(self, text):
        if not isinstance(text, str) or not text or str(text).upper() == 'NULL':
            return pd.Series([np.nan, np.nan])
        
        # Làm sạch: Xóa chấm và phẩy
        clean_text = text.lower().replace('.', '').replace(',', '') 
        
        nums = re.findall(r'\d+', clean_text)
        nums = [float(n) for n in nums]
        
        if not nums: return pd.Series([np.nan, np.nan])
        
        min_val, max_val = np.nan, np.nan
        
        # Logic phân tích
        if any(kw in clean_text for kw in ['dưới', 'ít hơn', 'less than']):
            min_val = 0.0
            max_val = nums[0]
        elif any(kw in clean_text for kw in ['trên', 'hơn', 'over', '+']):
            min_val = nums[0]
            max_val = np.nan 
        elif len(nums) >= 2:
            nums.sort()
            min_val, max_val = nums[0], nums[-1]
        elif len(nums) == 1:
            min_val, max_val = nums[0], nums[0]
            
        return pd.Series([min_val, max_val])
    # --- HÀM PHỤ TRỢ 2: PHÂN LOẠI KHOẢNG LƯƠNG (BINNING) ---
    def _get_salary_range_label(self, avg_salary):
        if avg_salary == 0 or pd.isna(avg_salary): return "Thỏa thuận"
        m = avg_salary / 1_000_000
        if m < 3: return "Dưới 3 triệu"
        elif 3 <= m < 10: return "3 - 10 triệu"
        elif 10 <= m < 15: return "10 - 15 triệu"
        elif 15 <= m < 25: return "15 - 25 triệu"
        elif 25 <= m < 35: return "25 - 35 triệu"
        elif 35 <= m < 50: return "35 - 50 triệu"
        else: return "Hơn 50 triệu"
    # ==========================================================================
    # CÁC HÀM LOGIC PHỤ TRỢ (HELPER METHODS) - CẦN BỔ SUNG
    # ==========================================================================
    # --- HÀM PHỤ TRỢ: XỬ LÝ HÌNH THỨC & KIỂU LÀM VIỆC (NÂNG CAO) ---
    def _extract_working_style(self, row):
        # 1. Lấy dữ liệu và gộp lại
        hinh_thuc_raw = row.get('HinhThucLamViec')
        mo_ta_raw = row.get('MoTaCongViec')

        if pd.isna(hinh_thuc_raw) and pd.isna(mo_ta_raw):
            # Nếu cả hai cột nguồn đều rỗng, trả về mặc định an toàn (2 giá trị)
            return pd.Series(['Full-time', 'Onsite']) 
        
        # Nếu không phải NaN, chuyển về string và lower
        hinh_thuc = str(hinh_thuc_raw).lower() if pd.notna(hinh_thuc_raw) else ""
        mo_ta = str(mo_ta_raw).lower() if pd.notna(mo_ta_raw) else ""
        
        full_text = f"{hinh_thuc} {mo_ta}"
        
        # PHẦN 1: XÁC ĐỊNH HÌNH THỨC (Full-time / Part-time / Freelance)
        emp_type = "Full-time" # Mặc định
        
        kw_freelance = ['freelance', 'freelancer', 'tự do', 'cộng tác viên', 'ctv', 'project base', 'theo dự án', 'thời vụ']
        if any(k in full_text for k in kw_freelance):
            emp_type = "Freelance"
        elif any(k in full_text for k in ['part time', 'part-time', 'bán thời gian', 'ca gãy', '4 tiếng', 'parttime']):
            emp_type = "Part-time"
        elif any(k in full_text for k in ['full time', 'full-time', 'toàn thời gian', 'chính thức', 'hành chính']):
            emp_type = "Full-time"

        # PHẦN 2: XÁC ĐỊNH KIỂU LÀM VIỆC (Onsite / Remote / Hybrid)
        work_mode = "Onsite" # Mặc định
        
        kw_hybrid = [
            'hybrid', 'linh hoạt', 'xen kẽ', 'flexible', 'kết hợp', 'mix', 
            'bán từ xa', 'semi-remote', 'ngày lên văn phòng', 'days at office'
        ]
        
        if any(k in full_text for k in kw_hybrid):
            work_mode = "Hybrid"
        elif any(k in full_text for k in ['remote', 'từ xa', 'wfh', 'work from home', 'tại nhà', 'không cần lên văn phòng']):
            work_mode = "Remote"
        elif any(k in full_text for k in ['onsite', 'tại văn phòng', 'office', 'trực tiếp', 'offline']):
            work_mode = "Onsite"

        # Trả về 2 giá trị tương ứng với 2 cột output
        return pd.Series([emp_type, work_mode])
    # 1. Xử lý Chức danh (Title)
    def clean_title(self, text):
        if not isinstance(text, str): return "Khác"
        text = text.lower()
        # Loại bỏ từ rác thường gặp trong tiêu đề
        for pat in [r'tuyển', r'gấp', r'hcm', r'hn', r'fulltime', r'remote', r'lương', r'tại', r'\[.*?\]', r'\(.*?\)']: 
            text = re.sub(pat, '', text)
        text = text.strip()
        
        # Mapping theo từ điển
        for std, kws in self.job_title_map.items():
            if any(k in text for k in kws): return std
        return "Khác"

    # 2. Xử lý Cấp bậc (Rank) - Đây là hàm bạn đang bị thiếu gây lỗi
    def clean_rank(self, text):
        t = str(text).lower()
        if any(x in t for x in ['thực tập', 'intern', 'trainee']): return "Thực tập sinh"
        if any(x in t for x in ['giám đốc', 'director', 'ceo', 'c-level', 'head of']): return "Giám đốc"
        if any(x in t for x in ['phó giám đốc', 'vp ', 'vice president']): return "Phó giám đốc"
        if any(x in t for x in ['trưởng phòng', 'manager', 'lead', 'trưởng nhóm', 'quản lý']): return "Trưởng phòng"
        return "Nhân viên"

    # 3. Xử lý Ngành nghề (Industry)
    def clean_industry(self, text):
        text = str(text).lower()
        for cat, kws in self.industry_map.items():
            if any(k in text for k in kws): return cat
        return "Khác"

    # 4. Xử lý Số lượng tuyển (Quantity) - ĐÃ CẬP NHẬT
    def clean_quantity(self, row):
        DEFAULT_QTY = 1

        # ==========================================
        # BƯỚC 1: XỬ LÝ CỘT 'SoLuongTuyen' (Ưu tiên 1)
        # ==========================================
        qty_from_col = DEFAULT_QTY
        raw_col = str(row.get('SoLuongTuyen')) if pd.notna(row.get('SoLuongTuyen')) else ""
        raw_col_lower = raw_col.lower()

        # THÊM: Kiểm tra các từ khóa chỉ số lượng lớn
        kw_bulk = ['nhiều', 'số lượng lớn', 'vô hạn', 'không giới hạn', 'hàng loạt']
        if any(k in raw_col_lower for k in kw_bulk):
            return 999 # Gán số lượng lớn cố định

        # Tìm số trong cột SoLuong
        col_matches = re.findall(r'\d+', raw_col)
        if col_matches:
            # Lọc để tránh bắt nhầm năm, chỉ lấy số nhỏ (< 1000)
            nums = [int(x) for x in col_matches if int(x) < 1000]
            if nums:
                qty_from_col = max(nums)

        # Nếu cột SoLuong đã ghi rõ ràng > 1, ta tin tưởng
        if qty_from_col > DEFAULT_QTY:
            return qty_from_col

        # ==========================================
        # BƯỚC 2: XỬ LÝ CỘT 'TenCongViec' (Fallback)
        # ==========================================
        title = str(row.get('CongViec')).lower() if pd.notna(row.get('CongViec')) else ""
        qty_from_title = DEFAULT_QTY

        # Định nghĩa các mẫu câu
        patterns = [
            r'tuyển\s+(\d+)',           
            r'(\d+)\s+vị trí',           
            r'(\d+)\s+nhân sự',          
            r'(\d+)\s+nhân viên',        
            r'(\d+)\s+người',            
            r'(\d+)\s+bạn',              
            r'(\d+)\s+slot',             
            r'(\d+)\s+kỹ sư',
            r'(\d+)\s+chuyên viên',
            r'(\d+)\s+kỹ thuật viên',
            r'số lượng\s*[:\-]?\s*(\d+)'
        ]

        found_nums = []
        for pat in patterns:
            match = re.search(pat, title)
            if match:
                val = int(match.group(1))
                if DEFAULT_QTY < val < 200:
                    found_nums.append(val)

        if found_nums:
            qty_from_title = max(found_nums)

        # ==========================================
        # BƯỚC 3: KẾT LUẬN
        # ==========================================
        # Chọn giá trị cao nhất từ hai nguồn
        return max(qty_from_col, qty_from_title)

    # 5. Xử lý Học vấn (Education)
    def find_education_coalesced(self, row):
        primary_edu = row.get('HocVan')
        secondary_req = row.get('YeuCauUngVien')
        
        
        # Hàm phụ trợ để kiểm tra nếu chuỗi có nội dung
        def is_valid_string(text):
            return pd.notna(text) and isinstance(text, str) and text.strip()

        # 1. Kiểm tra cột chính (HocVan)
        if is_valid_string(primary_edu):
            return primary_edu
        
        # 2. Kiểm tra cột phụ (YeuCauUngVien)
        if is_valid_string(secondary_req):
            return secondary_req
        
        # 4. Cuối cùng, trả về None (sau đó logic phân loại sẽ gán là "Khác")
        return "Không yêu cầu"

    def clean_education(self, text):
        t = str(text).lower()
        if 'trung cấp' in t: return "Trung cấp"
        if 'cao đẳng' in t: return "Cao đẳng"
        if any(x in t for x in ['đại học', 'cử nhân', 'kỹ sư', 'bachelor']): return "Đại học"
        if any(x in t for x in ['thạc sĩ', 'master']): return "Thạc sĩ"
        return "Không yêu cầu"
    
    # 6. Xử lý Kỹ năng (Skills)
    def clean_skills(self, row):
        txt = (str(row.get('MoTaCongViec', '')) + " " + str(row.get('YeuCauUngVien', ''))).lower()
        # Quét Hard Skills
        h = []
        for k, keywords in self.skill_map.get('hard', {}).items():
            for kw in keywords:
                # Dùng regex boundary để tránh bắt nhầm từ con
                if re.search(r'(?:^|\W)(' + kw + r')(?:$|\W)', txt):
                    h.append(k)
                    break # Tìm thấy 1 keyword của nhóm này là đủ
                    
        # Quét Soft Skills
        s = []
        for k, keywords in self.skill_map.get('soft', {}).items():
            for kw in keywords:
                if re.search(r'(?:^|\W)(' + kw + r')(?:$|\W)', txt):
                    s.append(k)
                    break

        return pd.Series([", ".join(sorted(h)), ", ".join(sorted(s))])

    # 7. Hàm phụ: Phân loại khoảng lương (Labeling)
    def _get_salary_range_label(self, avg_salary):
        if avg_salary == 0 or pd.isna(avg_salary): return "Thỏa thuận"
        m = avg_salary / 1_000_000
        if m < 3: return "Dưới 3 triệu"
        elif 3 <= m < 10: return "3 - 10 triệu"
        elif 10 <= m < 15: return "10 - 15 triệu"
        elif 15 <= m < 25: return "15 - 25 triệu"
        elif 25 <= m < 35: return "25 - 35 triệu"
        elif 35 <= m < 50: return "35 - 50 triệu"
        else: return "Hơn 50 triệu"

    # 8. Hàm phụ: Parse Kinh nghiệm (Numerics)
    def _extract_experience_numerics(self, raw_exp):
        if not isinstance(raw_exp, str) or not raw_exp: return pd.Series([None, None])
        text = raw_exp.lower().strip()
        if any(kw in text for kw in ['không yêu cầu', 'no experience', 'chưa có']): return pd.Series([0.0, 0.0])

        matches = re.findall(r'\d+(?:\.\d+)?', text)
        nums = [float(n) for n in matches]
        if not nums: return pd.Series([None, None])

        if 'tháng' in text and 'năm' not in text: nums = [n / 12 for n in nums]
        
        min_exp, max_exp = None, None
        if 'dưới 1 năm' in text: min_exp, max_exp = 0.0, 1.0
        elif any(kw in text for kw in ['trên', 'hơn', 'over']): min_exp, max_exp = nums[0], None
        elif any(kw in text for kw in ['dưới', 'less']): min_exp, max_exp = 0.0, nums[0]
        elif len(nums) >= 2: nums.sort(); min_exp, max_exp = nums[0], nums[-1]
        elif len(nums) == 1: min_exp, max_exp = nums[0], nums[0]
        
        return pd.Series([min_exp, max_exp])

    # 9. Hàm phụ: Parse Quy mô (Numerics)
    def _extract_size_numerics(self, text):
        if not isinstance(text, str) or not text: return pd.Series([np.nan, np.nan])
        clean_text = text.lower().replace('.', '').replace(',', '')
        nums = [float(n) for n in re.findall(r'\d+', clean_text)]
        if not nums: return pd.Series([np.nan, np.nan])
        
        if any(kw in clean_text for kw in ['dưới', 'ít hơn']): return pd.Series([0.0, nums[0]])
        if any(kw in clean_text for kw in ['trên', 'hơn']): return pd.Series([nums[0], np.nan])
        
        if len(nums) >= 2: 
            nums.sort()
            return pd.Series([nums[0], nums[-1]])
        return pd.Series([nums[0], nums[0]])
    def run(self):
        print("⏳ [1/5] Tải dữ liệu...")
        df = pd.read_sql("SELECT * FROM fact_jobpostings b WHERE NgayCaoDuLieu = CURDATE();", self.engine)
        
        print("⏳ [2/5] Xử lý dữ liệu cơ bản (Tạo cột cần thiết cho GroupBy)...")
        
        # 1. TẠO CÁC CỘT CƠ BẢN TRƯỚC (QUAN TRỌNG: Phải có CapBac_clean và CongViec_clean trước khi tính lương)
        df['CongTy'] = df['CongTy'].astype(str).str.strip().str.title()
        df['CongTy_clean'] = df['CongTy'].astype(str).str.strip().str.title()
        df['CongViec_clean'] = df['CongViec'].apply(self.clean_title)
        df['CapBac_clean'] = df['CongViec'].apply(self.clean_rank) # <--- ĐƯA LÊN ĐÂY
        
        df['LinhVuc_clean'] = df['LinhVuc'].apply(self.clean_industry)
        df['HocVan_clean'] = df['HocVan'].apply(self.clean_education)
        df['SoLuongTuyen_clean'] = df.apply(self.clean_quantity, axis=1)
        df['HinhThucLamViec_clean'] = df['HinhThucLamViec'].fillna('Toàn thời gian')
        df[['HinhThucLamViec_clean', 'KieuLamViec_clean']] = df.apply(self._extract_working_style, axis=1)
        df[['YeuCauKiNangCung_clean', 'YeuCauKiNangMem_clean']] = df.apply(self.clean_skills, axis=1)
        df['NgayCaoDuLieu'] = pd.to_datetime(df['NgayCaoDuLieu'], errors='coerce').dt.date
        
        # Xử lý hạn nộp (Sử dụng hàm clean_deadline và ép về date)
        df['HanNopHoSo_clean'] = df.apply(self.clean_deadline, axis=1)
        df['HanNopHoSo_clean'] = pd.to_datetime(df['HanNopHoSo_clean'], errors='coerce').dt.date

        print("⏳ [3] Xử lý Lương: Extract -> Imputation -> Binning...")
        # B1: Extract số liệu thô
        df[['MucLuongMin_clean', 'MucLuongMax_clean']] = df['MucLuong'].astype(str).apply(self._extract_salary_numerics)
        
        # B2: Điền giá trị thiếu (Imputation Logic)
        cols_sal = ['MucLuongMin_clean', 'MucLuongMax_clean']
        df[cols_sal] = df[cols_sal].replace(0, np.nan)
        
        # Bây giờ CapBac_clean đã tồn tại nên dòng này sẽ chạy mượt
        df['MucLuongMin_clean'] = df['MucLuongMin_clean'].fillna(
            df.groupby(['CongViec_clean', 'CapBac_clean'])['MucLuongMin_clean'].transform('mean')
        )
        df['MucLuongMax_clean'] = df['MucLuongMax_clean'].fillna(
            df.groupby(['CongViec_clean', 'CapBac_clean'])['MucLuongMax_clean'].transform('mean')
        )
        
        df[cols_sal] = df[cols_sal].fillna(0)
        
        # B3: Tính trung bình & Tạo cột Khoảng Lương
        df['MucLuongTB_clean'] = (df['MucLuongMin_clean'] + df['MucLuongMax_clean']) / 2
        df['KhoangLuong'] = df['MucLuongTB_clean'].apply(self._get_salary_range_label)

        print("⏳ [4] Xử lý Kinh nghiệm & Quy mô...")
        # --- KINH NGHIỆM ---
        
        def calc_avg_exp(row):
            mi, ma = row['YeuCauKinhNghiemMin_clean'], row['YeuCauKinhNghiemMax_clean']
            if pd.isna(mi) and pd.isna(ma): return np.nan
            if pd.isna(ma): return float(mi)
            if pd.isna(mi): return float(ma)
            return (float(mi) + float(ma)) / 2
            
        df[['YeuCauKinhNghiemMin_clean', 'YeuCauKinhNghiemMax_clean']] = df.apply(
            self._find_experience_coalesced, axis=1
        )
        df['YeuCauKinhNghiemTB_clean'] = df.apply(calc_avg_exp, axis=1)
        exp_bins = [0, 1, 3, 5, float('inf')]
        exp_labels = ["Dưới 1 năm", "1 – 3 năm", "3 – 5 năm", "Trên 5 năm"]
        df['PhanLoaiKinhNghiem'] = pd.cut(df['YeuCauKinhNghiemTB_clean'], bins=exp_bins, labels=exp_labels, right=False)
        df['PhanLoaiKinhNghiem'] = df['PhanLoaiKinhNghiem'].astype(str)
        df.loc[df['YeuCauKinhNghiemTB_clean'] == 0, 'PhanLoaiKinhNghiem'] = 'Không yêu cầu kinh nghiệm'
        df['PhanLoaiKinhNghiem'] = df['PhanLoaiKinhNghiem'].replace('nan', 'Khác')

        # --- QUY MÔ ---
        df[['QuyMoCongTyMin_clean', 'QuyMoCongTyMax_clean']] = df['QuyMoCongTy'].apply(self._extract_size_numerics)
        
        def calc_avg_size(row):
            mi, ma = row['QuyMoCongTyMin_clean'], row['QuyMoCongTyMax_clean']
            if pd.isna(mi): return np.nan
            if pd.isna(ma): return mi
            return (mi + ma) / 2
        
        df['QuyMoCongTyTB_clean'] = df.apply(calc_avg_size, axis=1)
        
        size_bins = [0, 10, 100, 500, 1000, 5000, float('inf')]
        size_labels = [
            "Dưới 10 nhân viên", "10 - 100 nhân viên", "100 - 500 nhân viên", 
            "500 - 1000 nhân viên", "1000 - 5000 nhân viên", "Trên 5000 nhân viên"
        ]
        df['PhanLoaiQuyMoCongTy'] = pd.cut(df['QuyMoCongTyTB_clean'], bins=size_bins, labels=size_labels, right=False)
        df['PhanLoaiQuyMoCongTy'] = df['PhanLoaiQuyMoCongTy'].astype(str).replace('nan', 'Không xác định')

        print("⏳ [3/5] Xử lý Địa điểm (QUAN TRỌNG)...")
        df_final = self.clean_location_data(df)
        df_final['NgayXuLyDL'] = datetime.now()

        output_cols =[
            # --- 1. ĐỊNH DANH ---
            'JobID', 'JobHash', 'CongTy', 'CongTy_clean', 'LinkBaiTuyenDung',
            # --- 2. CÔNG VIỆC ---
            'CongViec', 'CongViec_clean', 'CapBac', 'CapBac_clean',
            # --- 3. ĐỊA ĐIỂM ---
            'ViTri', 'ViTri_clean', 'Tinh_Thanh', 'KhuVuc', 'Latitute', 'longtitute',
            # 4. LƯƠNG
            'MucLuong', 'MucLuongMin_clean', 'MucLuongTB_clean', 'MucLuongMax_clean', 'KhoangLuong',
            'MoTaCongViec',         # <--- Thêm cột này
            'YeuCauUngVien',
            # 5. KINH NGHIỆM
            'YeuCauKinhNghiem', 'YeuCauKinhNghiemMin_clean', 'YeuCauKinhNghiemTB_clean', 'YeuCauKinhNghiemMax_clean', 'PhanLoaiKinhNghiem',
            # 6. KỸ NĂNG
            'YeuCauKiNang', 'YeuCauKiNangCung_clean', 'YeuCauKiNangMem_clean',
            # 7. KHÁC
            'LinhVuc', 'LinhVuc_clean',
            'HocVan', 'HocVan_clean',
            'HinhThucLamViec', 'HinhThucLamViec_clean', 'KieuLamViec_clean',
            'SoLuongTuyen', 'SoLuongTuyen_clean',
            'QuyMoCongTy', 'QuyMoCongTyMin_clean', 'QuyMoCongTyTB_clean', 'QuyMoCongTyMax_clean', 'PhanLoaiQuyMoCongTy',
            # 11. METADATA
            'HanNopHoSo', 'HanNopHoSo_clean',
            'Nguon', 'NgayCaoDuLieu', 'NgayXuLyDL'
        ]
        
        # Đảm bảo đủ cột
        for col in output_cols:
            if col not in df_final.columns: df_final[col] = None

        print("⏳ [5/5] Lưu Database...")
        df_final[output_cols].to_sql('fact_jobpostings_clean', self.engine, if_exists='append', index=False, chunksize=1000)
        print(f"✅ Hoàn tất! Đã lưu {len(df_final)} dòng.")
        return df_final
if __name__ == "__main__":
    pipeline = RecruitmentETL(DATABASE_URL)
    df = pipeline.run()