import sys
import os
import pandas as pd
import numpy as np
import re
import sqlalchemy
import hashlib
from datetime import datetime, timedelta
from sqlalchemy import text




# 1. CẤU HÌNH & IMPORT TỪ CONFIG
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, '..')) 
if project_root not in sys.path:
    sys.path.append(project_root)
try:
    from pipeline.config import DATABASE_URL
    print(" Connect từ pipeline/config.py thành công!")
except ImportError:
    sys.path.append(current_dir)
    try:
        from pipeline.config import DATABASE_URL
        print("Đã lấy Connection String từ pipeline/config.py (tại root) thành công!")
    except ImportError as e:
        print(f"Lỗi import config: {e}")
        DATABASE_URL = None

# 2. TOÀN BỘ MAPPING CỦA BẠN
JOB_ROLE_MAP = {
    # 1. Software Development
    "Back-end Developer": ["back-end", "backend", "java developer", "python developer", "php developer", "node"],
    "Front-end Developer": ["front-end", "frontend", "reactjs", "angular", "vuejs", "javascript developer"],
    "Full-stack Developer": ["full-stack", "fullstack"],
    "Mobile Developer": ["mobile developer", "ios", "android", "flutter", "react native"],
    "Game Developer": ["game developer", "unity", "cocos", "unreal"],
    "Embedded Engineer": ["embedded", "nhúng", "firmware"],
    
    # 2. Product Management
    "Product Owner/Product Manager": ["product owner", "po", "product manager", "pm "],
    "Business Analyst": ["business analyst", "ba ", "phân tích nghiệp vụ"],
    
    # 3. Management & Consulting
    "Project Leader/Project Manager": ["project leader", "project manager", "quản trị dự án"],
    "IT Manager": ["it manager", "trưởng phòng it"],
    "Tech Lead": ["tech lead", "technical lead"],
    "IT Consultant": ["it consultant", "tư vấn it"],
    
    # 4. Designing
    "Designer": ["designer", "thiết kế", "ui/ux", "figma"],
    
    # 5. Testing
    "Tester": ["tester", "kiểm thử"],
    "QA - QC": ["qa", "qc", "quality assurance"],
    
    # 6. Cloud & Infrastructure
    "System Engineer/System Admin": ["system engineer", "system admin", "sysadmin", "quản trị hệ thống"],
    "DevOps Engineer": ["devops", "devsecops", "sre"],
    "Cloud Engineer": ["cloud engineer", "aws", "azure", "gcp"],
    
    # 7. Data Analytics
    "Data Engineer": ["data engineer", "kỹ sư dữ liệu"],
    "Data Analyst/Data Scientist": ["data analyst", "data scientist", "bi analyst", "phân tích dữ liệu"],
    "Database Engineer": ["database engineer", "dba", "quản trị cơ sở dữ liệu"],
    
    # 8. AI & Blockchain
    "AI Engineer/Blockchain Engineer": ["ai engineer", "machine learning", "blockchain", "trí tuệ nhân tạo"],
    
    # 9. Others
    "ERP Engineer/ERP Consultant": ["erp", "sap", "odoo"],
    "Solution Architect": ["solution architect", "kiến trúc giải pháp"],
    "IT Support/Helpdesk": ["it support", "helpdesk", "hỗ trợ kỹ thuật"]
}
MERGE_MAP = {
     # --- Nhóm 1: Miền Tây & Nam Bộ ---
    "kiên giang": "An Giang", "an giang": "An Giang",
    "bạc liêu": "Cà Mau", "cà mau": "Cà Mau",
    "bình phước": "Đồng Nai", "đồng nai": "Đồng Nai",
    "tiền giang": "Đồng Tháp", "đồng tháp": "Đồng Tháp",
    "long an": "Tây Ninh", "tây ninh": "Tây Ninh",
    "bến tre": "Vĩnh Long", "trà vinh": "Vĩnh Long", "vĩnh long": "Vĩnh Long",
    "sóc trăng": "TP. Cần Thơ", "hậu giang": "TP. Cần Thơ", "cần thơ": "TP. Cần Thơ", 
    "tp. cần thơ": "TP. Cần Thơ",

    # --- Nhóm 2: Miền Trung & Tây Nguyên ---
    "phú yên": "Đắk Lắk", "đắk lắk": "Đắk Lắk", "dak lak": "Đắk Lắk",
    "bình định": "Gia Lai", "gia lai": "Gia Lai",
    "ninh thuận": "Khánh Hoà", "khánh hoà": "Khánh Hoà", "nha trang": "Khánh Hoà",
    "đắk nông": "Lâm Đồng", "dak nong": "Lâm Đồng", "bình thuận": "Lâm Đồng", "lâm đồng": "Lâm Đồng", "đà lạt": "Lâm Đồng",
    "kon tum": "Quảng Ngãi", "quảng ngãi": "Quảng Ngãi",
    "quảng bình": "Quảng Trị", "quảng trị": "Quảng Trị",
    "quảng nam": "TP. Đà Nẵng", "đà nẵng": "TP. Đà Nẵng", "tp. đà nẵng": "TP. Đà Nẵng",
    "thừa thiên huế": "TP. Huế", "huế": "TP. Huế", "tp. huế": "TP. Huế",

    # --- Nhóm 3: Miền Bắc ---
    "bắc giang": "Bắc Ninh", "bắc ninh": "Bắc Ninh",
    "thái bình": "Hưng Yên", "hưng yên": "Hưng Yên",
    "yên bái": "Lào Cai", "lào cai": "Lào Cai",
    "hà nam": "Ninh Bình", "nam định": "Ninh Bình", "ninh bình": "Ninh Bình",
    "hòa bình": "Phú Thọ", "vĩnh phúc": "Phú Thọ", "phú thọ": "Phú Thọ",
    "bắc kạn": "Thái Nguyên", "bắc cạn": "Thái Nguyên", "thái nguyên": "Thái Nguyên",
    "hà giang": "Tuyên Quang", "tuyên quang": "Tuyên Quang",
    "hải dương": "TP. Hải Phòng", "hải phòng": "TP. Hải Phòng", "tp. hải phòng": "TP. Hải Phòng",
    "hà nội": "TP. Hà Nội", "hn": "TP. Hà Nội", "tp. hà nội": "TP. Hà Nội",

    # --- Nhóm 4: TP. HCM ---
    "bình dương": "TP. Hồ Chí Minh", "bà rịa": "TP. Hồ Chí Minh", "vũng tàu": "TP. Hồ Chí Minh",
    "bà rịa - vũng tàu": "TP. Hồ Chí Minh", "hồ chí minh": "TP. Hồ Chí Minh", "hcm": "TP. Hồ Chí Minh",
    "tphcm": "TP. Hồ Chí Minh", "sg": "TP. Hồ Chí Minh", "tp. hồ chí minh": "TP. Hồ Chí Minh",

    # --- Nhóm 5: Các tỉnh giữ nguyên ---
    "cao bằng": "Cao Bằng", "điện biên": "Điện Biên", "hà tĩnh": "Hà Tĩnh",
    "lai châu": "Lai Châu", "lạng sơn": "Lạng Sơn", "nghệ an": "Nghệ An",
    "quảng ninh": "Quảng Ninh", "sơn la": "Sơn La", "thanh hóa": "Thanh Hóa"

}

COORD_MAP = {
    "TP. Hà Nội": ("Bắc", 21.0285, 105.8542),
    "Bắc Ninh": ("Bắc", 21.1861, 106.0763),
    "Hưng Yên": ("Bắc", 20.9333, 106.3167),
    "TP. Hải Phòng": ("Bắc", 20.8449, 106.6881),
    "Phú Thọ": ("Bắc", 21.3228, 105.2539),
    "Thái Nguyên": ("Bắc", 21.5672, 105.8244),
    "Ninh Bình": ("Bắc", 20.2541, 105.9751),
    "Quảng Ninh": ("Bắc", 20.9500, 107.0833),
    "Lào Cai": ("Bắc", 22.4856, 103.9707),
    "Tuyên Quang": ("Bắc", 21.8251, 105.2114),
    "Cao Bằng": ("Bắc", 22.6567, 106.2526),
    "Lạng Sơn": ("Bắc", 21.8333, 106.7333),
    "Sơn La": ("Bắc", 21.3265, 103.9048),
    "Điện Biên": ("Bắc", 21.3916, 103.0189),
    "Lai Châu": ("Bắc", 22.3929, 103.4735),

    # --- MIỀN TRUNG ---
    "TP. Đà Nẵng": ("Trung", 16.0544, 108.2022),
    "Nghệ An": ("Trung", 18.6734, 105.6791),
    "Thanh Hóa": ("Trung", 19.8077, 105.7765),
    "Hà Tĩnh": ("Trung", 18.3427, 105.9058),
    "TP. Huế": ("Trung", 16.4637, 107.5909),
    "Quảng Trị": ("Trung", 16.7686, 107.1822),
    "Quảng Ngãi": ("Trung", 15.1205, 108.7923),
    "Khánh Hoà": ("Trung", 12.2388, 109.1967),
    "Gia Lai": ("Trung", 13.9833, 108.0000),
    "Lâm Đồng": ("Trung", 11.9404, 108.4583),
    "Đắk Lắk": ("Trung", 12.6667, 108.0500),

    # --- MIỀN NAM ---
    "TP. Hồ Chí Minh": ("Nam", 10.8231, 106.6297),
    "Đồng Nai": ("Nam", 10.9574, 106.8427),
    "Tây Ninh": ("Nam", 11.3667, 106.1167),
    "TP. Cần Thơ": ("Nam", 10.0452, 105.7469),
    "Vĩnh Long": ("Nam", 10.2541, 105.9723),
    "An Giang": ("Nam", 10.3717, 105.4323),
    "Cà Mau": ("Nam", 9.1769, 105.1524),
    "Đồng Tháp": ("Nam", 10.5333, 105.6833),
    "Khác": ("Khác", 16.0, 112.0, "Khác")

}
JOB_TITLE_MAP = {
    "Project Manager": ["project manager", "pm ", "quản trị dự án"],
    "Business Analyst": ["business analyst", "ba ", "phân tích nghiệp vụ"],
    "Developer": ["developer", "lập trình viên", "coder", "kỹ sư phần mềm"],
    "Tester": ["tester", "qa", "qc", "kiểm thử"],
    "Data Analyst": ["data analyst", "da ", "phân tích dữ liệu"],
    "System Admin": ["system admin", "sysadmin", "quản trị mạng"],
    "Designer": ["designer", "thiết kế", "ui/ux"]

}
SKILL_MAP = {
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
        "SQL": ["sql", "mysql", "postgres", "sql server", "nosql", "mongodb", "redis"], 
        "HTML/CSS": ["html", "css"],
        "Rust": ["rust"],
        "Scala": ["scala"],
        "Bash/Shell": ["bash", "shell script", "linux"],
        "PowerShell": ["powershell"],
        "VBA": ["vba", "excel macro"],
        "MATLAB": ["matlab"],
        
        # --- Framework/Lib ---
        "React": ["react", "reactjs", "react.js", "react native"],
        "Angular": ["angular"],
        "Vue": ["vue", "vuejs"],
        "NodeJS": ["node", "nodejs", "node.js"],
        "Spring": ["spring boot", "spring mvc"],
        "Django/Flask": ["django", "flask"],
        
        # --- Cloud & DevOps ---
        "AWS": ["aws", "amazon web services"],
        "Azure": ["azure"],
        "GCP": ["gcp", "google cloud"],
        "Docker": ["docker"],
        "Kubernetes": ["k8s", "kubernetes"],
        "Git": ["git", "github", "gitlab", "svn"],
        "CI/CD": ["ci/cd", "jenkins", "gitlab ci"],
        
        # --- Data & Analytics ---
        "Excel": ["excel", "spreadsheet", "vlookup", "pivot table"],
        "Power BI": ["power bi", "powerbi", "dax"],
        "Tableau": ["tableau"],
        "Spark": ["spark", "pyspark"],
        "Hadoop": ["hadoop", "hive"],
        
        # --- Design & Tools ---
        "Jira": ["jira", "confluence"],
        "Figma/Design": ["figma", "photoshop", "adobe xd", "sketch"]
    },
    "soft": {
         # --- Giao tiếp & Lãnh đạo ---
        "Giao tiếp": ["giao tiếp", "communication", "trình bày", "thuyết trình"],
        "Lãnh đạo/Quản lý": ["lãnh đạo", "leadership", "dẫn dắt", "quản lý nhóm", "team lead", "thúc đẩy"],
        "Thương lượng": ["thương lượng", "đàm phán", "negotiation"],
        
        # --- Tư duy ---
        "Giải quyết vấn đề": ["giải quyết vấn đề", "problem solving", "xử lý tình huống"],
        "Tư duy phản biện": ["phản biện", "critical thinking", "tư duy logic", "tư duy hệ thống"],
        "Sáng tạo": ["sáng tạo", "creative", "innovation", "ý tưởng mới"],
        "Phân tích": ["kỹ năng phân tích", "analytical"],
        
        # --- Thái độ & Cách làm việc ---
        "Quản lý thời gian": ["quản lý thời gian", "time management", "đúng hạn", "deadline"],
        "Làm việc nhóm": ["làm việc nhóm", "teamwork", "team work", "hòa đồng", "phối hợp"],
        "Chịu áp lực": ["chịu được áp lực", "work under pressure", "áp lực cao"],
        "Tự học/Chủ động": ["tự học", "self-learning", "thích nghi", "ham học hỏi", "chủ động", "cầu tiến"],
        "Chi tiết/Cẩn thận": ["tỉ mỉ", "cẩn thận", "chi tiết", "detail-oriented"]
    },
    "language": {
    "Tiếng Anh": ["tiếng anh", "english", "toeic", "ielts", "toefl", "đọc hiểu tài liệu"],
        "Tiếng Nhật": ["tiếng nhật", "japanese", "n1", "n2", "n3"],
        "Tiếng Trung": ["tiếng trung", "chinese", "hsk", "hoa ngữ"],
        "Tiếng Hàn": ["tiếng hàn", "korean", "topik"]
}

}


EDU_MAP = {
    'Tiến sĩ': ['tiến sĩ', 'doctorate', 'phd'],
    'Thạc sĩ': ['thạc sĩ', 'master', 'mba'],
    'Đại học': ['đại học', 'cử nhân', 'kỹ sư', 'bachelor', 'university'],
    'Cao đẳng': ['cao đẳng', 'college'],
    'Trung cấp': ['trung cấp', 'intermediate'],
    'Tốt nghiệp phổ thông': ['tốt nghiệp thpt', 'cấp 3', '12/12'],
    'Chứng chỉ':[ 'ceh', 'ecsa', 'iso 27001', 'ccna security', 'ccnp security', 'comptia security','comptia network+', 'ccna', 'ccnp enterprise', 'ccie','oscp', 'cissp', 'cisa', 'gcih', 'comptia security+', 'cysa+', 'pentest+','aws certified solutions architect', 'azure administrator az-104', 'google cloud engineer', 'aws cloud practitioner', 'azure fundamentals az-900','rhcsa', 'linux+', 'lpi linux essentials', 'google data analytics', 'tableau desktop specialist','itil 4 foundation', 'pmp', 'cka (kubernetes)', 'docker certified associate', 'devops engineer']
}
INDUSTRY_MAPPING_RULES = {
    "Tài chính - Ngân hàng": ["ngân hàng", "chứng khoán", "tài chính", "đầu tư", "bảo hiểm", "kế toán", "kiểm toán", "thuế"],
    "Sản xuất & Kỹ thuật": ["sản xuất", "vận hành sản xuất", "cơ khí", "ô tô", "tự động hóa", "điện / điện tử", "điện lạnh", "điện công nghiệp", "bảo trì", "sửa chữa", "dệt may", "da giày", "thời trang", "gỗ", "nội thất", "dầu khí", "khoáng sản", "năng lượng", "hóa học", "công nghiệp", "nông nghiệp", "nông lâm ngư nghiệp", "kỹ thuật ứng dụng", "quản lý chất lượng", "qa/qc"],
    "Y tế & Sức khỏe": ["y tế", "dược", "bệnh viện", "chăm sóc sức khỏe", "thẩm mỹ", "làm đẹp", "công nghệ sinh học", "hóa mỹ phẩm", "nha khoa"],
    "Xây dựng & Bất động sản": ["xây dựng", "bất động sản", "kiến trúc", "thiết kế nội thất", "vật liệu xây dựng"],
    "Thương mại & Bán lẻ": ["bán lẻ", "bán sỉ", "hàng tiêu dùng", "fmcg", "thực phẩm", "đồ uống", "hàng gia dụng", "chăm sóc cá nhân", "thương mại tổng hợp", "siêu thị", "thương mại điện tử", "e-commerce"],
    "Vận tải & Logistics": ["vận chuyển", "giao nhận", "kho vận", "logistics", "kho bãi", "hàng không", "xuất nhập khẩu", "thu mua", "vật tư", "chuỗi cung ứng"],
    "Dịch vụ & Giải trí": ["du lịch", "nhà hàng", "khách sạn", "nghệ thuật", "thiết kế", "giải trí", "truyền hình", "báo chí", "biên tập", "xuất bản", "in ấn", "tổ chức sự kiện"],
    "Giáo dục & Đào tạo": ["giáo dục", "đào tạo", "thư viện", "trường học", "trung tâm anh ngữ"],
    "Marketing & Truyền thông": ["marketing", "tiếp thị", "quảng cáo", "truyền thông", "đối ngoại", "pr", "agency", "digital marketing"],
    "Dịch vụ doanh nghiệp": ["nhân sự", "hành chính", "thư ký", "luật", "pháp lý", "biên phiên dịch", "thông dịch", "tư vấn", "dịch vụ khách hàng"],
    "Công nghệ & Viễn thông": ["cntt", "phần mềm", "phần cứng", "mạng", "viễn thông", "bưu chính viễn thông", "internet", "online", "game", "it - phần mềm", "it - phần cứng"],
    "Kinh doanh / Sales": ["bán hàng", "kinh doanh", "sales", "phát triển thị trường"]
}

# 3. CLASS ETL

class RecruitmentETL:
    def __init__(self, connection_string):
        self.engine = sqlalchemy.create_engine(connection_string)
        self.merge_map = MERGE_MAP
        self.coord_map = COORD_MAP
        self.skill_map = SKILL_MAP
        self.edu_map = EDU_MAP
        self.job_title_map = JOB_TITLE_MAP
        self.industry_map = self._init_industry_map()
        self.industry_map_mac =INDUSTRY_MAPPING_RULES
        self.job_role_map = JOB_ROLE_MAP
        print("Đã khởi tạo cấu hình ETL với đầy đủ Mapping.")
    def clean_job_role(self, text):
        if not isinstance(text, str) or not text:
            return "Khác"
        
        t = text.lower()
        # Duyệt qua map để tìm từ khóa phù hợp nhất
        for role, keywords in self.job_role_map.items():
            if any(kw in t for kw in keywords):
                return role
        return "Khác"
    def _init_industry_map(self):
        return {
            "Software Development": [
                "front-end", "frontend", "back-end", "backend", "fullstack", "full-stack",
                "software developer", "software engineer", "lập trình viên", "coder", "developer",
                "embedded", "nhúng", "firmware"
            ],
            "Product Management": [
                "product owner", "product manager", "po", "business analyst", "ba ", "phân tích nghiệp vụ"
            ],
            "Management & Consulting": [
                "project leader", "project manager", "pm ", "it manager", "tech lead", "it consultant", "quản trị dự án"
            ],
            "Designing": [
                "designer", "thiết kế", "ui/ux", "figma"
            ],
            "Testing": [
                "tester", "qa", "qc", "kiểm thử", "quality assurance"
            ],
            "Cloud & Infrastructure": [
                "system engineer", "system admin", "sysadmin", "devops", "cloud engineer", "aws", "azure", "network"
            ],
            "Data Analytics": [
                "data engineer", "data analyst", "data scientist", "bi analyst", "database", "phân tích dữ liệu"
            ],
            "AI & Blockchain": [
                "ai engineer", "machine learning", "blockchain", "trí tuệ nhân tạo", "deep learning"
            ],
            "Helpdesk": [
                "erp", "solution architect", "it support", "helpdesk", "hỗ trợ kỹ thuật"
            ],
            "IT - Khác": [
                "erp", "solution architect"
            ]
        }

    # --- LOGIC LƯU DATABASE ---
    def save_data_directly(self, df):

        if df.empty: return

        TABLE_NAME = "fact_clean_jobs"

        print(f" Đang nạp dữ liệu vào bảng {TABLE_NAME}...")
        try:
            df_to_save = df.copy()
            df_to_save['NgayCaoDuLieu'] = pd.to_datetime(df_to_save['NgayCaoDuLieu'], errors='coerce').dt.date
            df_to_save['HanNopHoSo_clean'] = pd.to_datetime(df_to_save['HanNopHoSo_clean'], errors='coerce').dt.date
            df_to_save['NgayXuLyDL'] = pd.to_datetime(df_to_save['NgayXuLyDL'], errors='coerce')
            text_cols = df_to_save.select_dtypes(include=['object']).columns
            for col in text_cols:
                df_to_save[col] = df_to_save[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
            dtype_map = {}
            df_to_save.to_sql(
                    TABLE_NAME,
                    self.engine,
                    schema="dbo",
                    if_exists="append",     
                    index=False,
                    chunksize=500,
                    dtype=dtype_map,
    )
            print(" Thành công!")
        except Exception as e:
            print(f" Lỗi ghi DB: {e}")
        except Exception as e:

            print(f" Lỗi ghi DB chi tiết: {e}")
            if "not available" in str(e):
                print(f" Kiểm tra tên bảng thực tế trong DB hoặc thử dùng if_exists='replace' một lần.")
    def clean_title(self, text):
        if not isinstance(text, str): return None 
        t = text.lower()
        noise = [r'tuyển\s*gấp', r'cần\s*tuyển', r'hot', r'urgent', r'lương\s*.*', r'\d+\s*-\s*\d+\s*(triệu|usd)']
        for pattern in noise: t = re.sub(pattern, ' ', t)
        t = re.sub(r'[^\w\s\-\+#\./&]', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        return t.title() if t else None
    def clean_quantity(self, row):
        DEFAULT_QTY = 1
        qty_from_col = DEFAULT_QTY
        raw_col = str(row.get('SoLuongTuyen')) if pd.notna(row.get('SoLuongTuyen')) else ""
        raw_col_lower = raw_col.lower()
        kw_bulk = ['nhiều', 'số lượng lớn', 'vô hạn', 'không giới hạn', 'hàng loạt']
        if any(k in raw_col_lower for k in kw_bulk):
            return 999 
        col_matches = re.findall(r'\d+', raw_col)
        if col_matches:
            nums = [int(x) for x in col_matches if int(x) < 1000]
            if nums:
                qty_from_col = max(nums)
        if qty_from_col > DEFAULT_QTY:

            return qty_from_col

        # XỬ LÝ CỘT 'TenCongViec'
        title = str(row.get('CongViec')).lower() if pd.notna(row.get('CongViec')) else ""
        qty_from_title = DEFAULT_QTY
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
        return max(qty_from_col, qty_from_title)
#Xử lý lương
    
    def clean_salary(self, raw_salary):
        if not isinstance(raw_salary, str) or not raw_salary:
            return 0.0, 0.0

        text = raw_salary.lower().strip()

        # mặc định: VNĐ, chỉ khi có usd hoặc $ mới là USD
        is_usd = ("usd" in text) or ("$" in text)

        text = (
            text.replace("triệu", "tr")
                .replace("nghìn", "k")
                .replace("million", "m")
                .replace("millions", "m")
        )

        text = re.sub(r'(?<=\d),(?=\d{3})', '', text)
        text = text.replace(',', '.')

        matches = re.findall(r'(\d+(?:\.\d+)?)(\s*[ktrm]?)', text)

        values = []
        for num, unit in matches:
            n = float(num)
            unit = unit.strip()

            if is_usd:
                if unit == "k":
                    n = n * 1000
                elif unit == "m":
                    n = n * 1_000_000
                n = n * 0.025
            else:
                if unit == "k":
                    n = n / 1000
                elif unit in ("tr", "m"):
                    n = n
                else:
                    # không có hậu tố → mặc định là VNĐ
                    n = n / 1_000_000

            values.append(round(n, 2))

        if not values:
            return 0.0, 0.0

        if len(values) == 1:
            v = values[0]
            if any(k in text for k in ["đến", "tới", "up to", "max", "dưới"]):
                return 0.0, v
            if any(k in text for k in ["từ", "min", "from", "trên"]):
                return v, 0.0
            return v, v

        return min(values), max(values)
    def process_interest_text(self, text):
        s_min, s_max = self.clean_salary(text)
        s_tb = (s_min + s_max) / 2 if (s_min > 0 and s_max > 0) else (s_min or s_max)
        label = self._get_salary_range_label(s_tb)
        return {"MucLuongMin": s_min, "MucLuongMax": s_max, "MucLuongTB": s_tb, "KhoangLuong": label}
    def _get_salary_range_label(self, avg_salary):
        if avg_salary == 0 or pd.isna(avg_salary): return "Thỏa thuận"
        m = avg_salary
        if m < 3: return "Dưới 3 triệu"
        elif 3 <= m < 10: return "3 - 10 triệu"
        elif 10 <= m < 20: return "10 - 20 triệu"
        elif 20 <= m < 35: return "20 - 35 triệu"
        elif 35 <= m < 50: return "35 - 50 triệu"
        else: return "Hơn 50 triệu"

    def _make_salary_clean(self, row):
        mi = row.get('MucLuongToiThieu', 0)
        ma = row.get('MucLuongToiDa', 0)
        mi = 0 if pd.isna(mi) else mi
        ma = 0 if pd.isna(ma) else ma
        if mi == 0 and ma == 0: 
            return "Thỏa thuận"
        if mi > 0 and ma > 0: 
            if mi == ma: return f"{mi:.1f} triệu"
            return f"{mi:.1f} - {ma:.1f} triệu"
        if mi > 0: 
            return f"Từ {mi:.1f} triệu"
        if ma > 0: 
            return f"Đến {ma:.1f} triệu"
   
        return "Thỏa thuận"
#Xử lý kinh nghiệm
    def _extract_experience_numerics_strict(self, text):
        if not isinstance(text, str) or any(kw in text.lower() for kw in ['không yêu cầu', 'no experience']):
            return 0.0, 0.0
        matches = re.findall(r'(\d+(?:\.\d+)?)\s*(?:năm|year)', text.lower())
        nums = [float(n) for n in matches]
        if not nums:
            return np.nan, np.nan 
        if len(nums) == 1:
            if "trên" in text.lower() or "hơn" in text.lower():
                return nums[0], np.nan
            if "dưới" in text.lower():
                return 0.0, nums[0]
            return nums[0], nums[0]
        return min(nums), max(nums)

    def _make_exp_clean(self, row):
        mi, ma = row['KinhNghiemToiThieu'], row['KinhNghiemToiDa']
        if pd.isna(mi) and pd.isna(ma): return "Không yêu cầu"
        if mi == 0 and ma == 0: return "Không yêu cầu"
        if mi is not None and ma is not None:
            if mi == ma: return f"{mi} năm"
            return f"{mi} - {ma} năm"
        if mi is not None: return f"Trên {mi} năm"
        if ma is not None: return f"Dưới {ma} năm"
        return "Không yêu cầu"
    
    # Xử lý Quy mô
    def _extract_size_numerics(self, text):
        if not isinstance(text, str) or not text: return pd.Series([np.nan, np.nan])
        clean_text = str(text).replace('.', '').replace(',', '')
        nums = re.findall(r'\d+', clean_text)
        nums = [float(n) for n in nums]
        if not nums: return pd.Series([np.nan, np.nan])
        if len(nums) >= 2:
            nums.sort()
            return pd.Series([min(nums), max(nums)])
        return pd.Series([nums[0], nums[0]])
    def _make_size_clean(self, row):
        mi, ma = row.get('QuyMoMin'), row.get('QuyMoMax')
        if pd.isna(mi): return "Không xác định"
        if mi == ma: return f"{int(mi)} nhân viên"
        return f"{int(mi)} - {int(ma)} nhân viên"
    def _binning_quy_mo(self, text):
        if not text or text == 'Không xác định':
            return 'Không xác định'
        
        # Loại bỏ dấu chấm/phẩy ngăn cách hàng nghìn trước khi tìm số
        clean_text = str(text).replace('.', '').replace(',', '')
        numbers = [int(s) for s in re.findall(r'\d+', clean_text)]
        
        if not numbers:
            return 'Không xác định'
        
        # Lấy giá trị lớn nhất để phân loại (ví dụ 100-500 nhân viên sẽ tính là 500)
        val = max(numbers)
        
        if val < 10: return 'Siêu nhỏ'
        elif val < 100: return 'Nhỏ'
        elif val < 500: return 'Trung bình thấp'
        elif val < 1000: return 'Trung bình cao'
        elif val < 5000: return 'Lớn'
        elif val < 10000: return 'Rất lớn'
        else: return 'Tập đoàn'
#Xử lý cấp bậc
    def _extract_rank_strict(self, text, exp_avg=None):
        if pd.isna(text) or text == '' or text == 'N/A': 
            rank_from_text = "Junior"
        else:
            t = str(text).lower()
            if any(x in t for x in ['thực tập', 'intern', 'sinh viên', 'cộng tác viên']): 
                rank_from_text = "Internship"
            elif any(x in t for x in ['leader', 'team lead', 'trưởng nhóm', 'manager', 'giám đốc', 'trưởng phòng', 'quản lý', 'giám sát', 'chi nhánh']): 
                rank_from_text = "Leader"
            elif any(x in t for x in ['project manager', ' pm']): 
                rank_from_text = "Project Manager"
            elif any(x in t for x in ['senior', 'sr.', 'cao cấp', 'chuyên gia']): 
                rank_from_text = "Senior"
            elif any(x in t for x in ['mid-level', 'middle']): 
                rank_from_text = "Mid-level"
            elif any(x in t for x in ['fresher', 'mới tốt nghiệp', 'mới đi làm']): 
                rank_from_text = "Fresher"
            else:
                rank_from_text = "Junior"
        if rank_from_text == "Junior" and exp_avg is not None and not pd.isna(exp_avg):
            if exp_avg >= 5: return "Senior"
            if 3 <= exp_avg < 5: return "Mid-level"
            if 1 <= exp_avg < 3: return "Junior"
            if exp_avg < 1: return "Fresher"
            
        return rank_from_text
#Xử lý loại hình lam việc
    def _determine_employment_type(self, text):
        t = str(text).lower()
        if any(k in t for k in ['freelance', 'tự do', 'ctv', 'thời vụ']): return "Freelance"
        if any(k in t for k in ['part time', 'bán thời gian','parttime']): return "Part-time"
        return "Full-time"
#Xử lý kiểu làm việc
    def _determine_work_mode(self, text):
        t = str(text).lower()
        if any(k in t for k in ['hybrid', 'linh hoạt']): return "Hybrid"
        if any(k in t for k in ['remote', 'từ xa', 'wfh']): return "Remote"
        return "Onsite"

#Xử lý lĩnh vực con
    def clean_industry(self, text):
        t = str(text).lower()
        for cat, kws in self.industry_map.items():
            if any(k in t for k in kws): return cat
        return "IT - Khác"
#Xử lý lĩnh vực web
    def clean_industry_macro(self, text):
        if pd.isna(text) or str(text).strip() == "":
            return "Khác" 
        t = str(text).lower()
        for group, keywords in self.industry_map_mac.items():
            if any(kw in t for kw in keywords):
                return group   
        return "Khác"
#xử lý học vấn
    def clean_education(self, text):
        if not isinstance(text, str) or not text: return "Không yêu cầu"
        t = text.lower()
        for level, keywords in self.edu_map.items():
            if any(k in t for k in keywords): return level
        return "Không yêu cầu"
    def find_education_coalesced(self, row):
        # Ưu tiên cột HocVan
        res = self.clean_education(row.get('HocVan'))

        # Nếu không có thì fallback sang YeuCauUngVien
        if res == "Không yêu cầu":
            res = self.clean_education(row.get('YeuCauUngVien'))

        return res

#Xử lý kĩ năng
    def _extract_skills(self, text, skill_type='hard'):
        if not isinstance(text, str): return "Không yêu cầu"
        found = []
        for k, keywords in self.skill_map[skill_type].items():
            for kw in keywords:
                if re.search(r'(?:^|\W)' + re.escape(kw) + r'(?:$|\W)', text.lower()):
                    found.append(k)
                    break
        return ", ".join(sorted(found)) if found else "Không yêu cầu"

#Xử lý hạn nộp
    def clean_deadline(self, row):
        raw = str(row.get('HanNopHoSo', '')).lower().strip()

        ref_date_raw = row.get('NgayCaoDuLieu')
        if pd.isna(ref_date_raw):
            ref_date = datetime.now()
        else:
            ref_date = pd.to_datetime(ref_date_raw)
            
        # Tính toán ngày mặc định (1 tháng sau ngày cào)
        default_date = (ref_date + timedelta(days=30)).strftime('%Y-%m-%d')
        # Kiểm tra nếu dữ liệu trống hoặc chứa từ khóa "thỏa thuận/null"
        if not raw or any(x in raw for x in ['na', 'null', 'thỏa thuận', 'không', 'nan']):
            return default_date
        match = re.search(r'(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})', raw)
        if match:
            try:
                d, m, y = map(int, match.groups())
                # Kiểm tra tính hợp lệ của ngày (tránh trường hợp 31/02)
                valid_date = datetime(y, m, d).strftime('%Y-%m-%d')
                return valid_date
            except ValueError:
                return default_date # Nếu ngày không hợp lệ (ví dụ 32/01) thì trả về mặc định
        # Nếu không khớp định dạng nào, trả về mặc định 1 tháng
        return default_date
#Xử lý vị trí
    def clean_location_data(self, df):
        print("   -> Đang tách địa điểm và lọc theo Map tỉnh thành...")
        
        # 1. Hàm kiểm tra xem một chuỗi có chứa từ khóa trong Map hay không
        def filter_valid_locations(location_string):
            # Tách chuỗi gốc thành danh sách các cụm từ theo dấu ; | ,
            raw_parts = [p.strip() for p in re.split(r'[;|,]', str(location_string)) if p.strip()]
            valid_parts = []
            
            for part in raw_parts:
                part_lower = part.lower()
                # Kiểm tra xem cụm từ này có chứa bất kỳ tỉnh nào trong MERGE_MAP không
                found = False
                for keyword, province_name in self.merge_map.items():
                    if keyword in part_lower:
                        valid_parts.append(part) # Giữ lại nguyên gốc để xử lý tiếp
                        found = True
                        break
                # Nếu cụm từ (part) không chứa từ khóa nào trong map -> Bị loại bỏ (Drop)
            
            return valid_parts

        # 2. Áp dụng tách và lọc
        df['Temp_Loc_List'] = df['ViTri'].apply(filter_valid_locations)
        
        # 3. Nhân bản dòng (Explode) - Chỉ những dòng có list không rỗng mới được giữ lại
        df_exploded = df.explode('Temp_Loc_List').dropna(subset=['Temp_Loc_List'])

        # 4. Hàm lấy thông tin Geo chi tiết cho những phần đã lọc
        def get_geo(loc_raw):
            loc_lower = str(loc_raw).lower()
            tinh_gop = None
            for k, v in self.merge_map.items():
                if k in loc_lower:
                    tinh_gop = v
                    break     
            region, lat, lng = "Khác", None, None
            if tinh_gop in self.coord_map:
                info = self.coord_map[tinh_gop]
                region, lat, lng = info[0], info[1], info[2]
            
            return pd.Series([loc_raw.title(), tinh_gop, region, lat, lng])

        # 5. Điền dữ liệu sạch vào các cột mới
        geo_cols = ['ViTri_clean', 'Tinh_Thanh', 'KhuVuc', 'Latitude', 'Longitude']
        if not df_exploded.empty:
            df_exploded[geo_cols] = df_exploded['Temp_Loc_List'].apply(get_geo)
            
            # Xử lý trùng lặp: Nếu sau khi tách 1 tin có 2 địa điểm cùng map về 1 tỉnh 
            # (VD: "Quận 1, TP.HCM" tách ra cả 2 đều thuộc HCM) thì chỉ lấy 1 dòng.
            df_exploded = df_exploded.drop_duplicates(subset=['LinkBaiTuyenDung', 'Tinh_Thanh'])
        
        # Xóa cột tạm
        df_exploded.drop(columns=['Temp_Loc_List'], inplace=True)
        
        print(f"   -> Hoàn tất: Giữ lại {len(df_exploded)} bản ghi hợp lệ.")
        return df_exploded
    def run(self):
        print("[1/3] Đang tải dữ liệu từ SQL Server...")
        query = "SELECT * FROM fact_jobpostings WHERE CAST(NgayCaoDuLieu AS DATE) = CAST(GETDATE() AS DATE);"
        #query = "SELECT * FROM fact_jobpostings"
        df = pd.read_sql(query, self.engine)
        if df.empty:
            print("Không có dữ liệu mới hôm nay."); return None
        print(f"Đã tải {len(df)} dòng. Đang thực hiện Transformation...")

        # --- 1. XỬ LÝ KINH NGHIỆM ---
        exp_df = df['YeuCauKinhNghiem'].apply(self._extract_experience_numerics_strict).apply(pd.Series)
        df['Exp_Min_clean'] = exp_df[0]
        df['Exp_Max_clean'] = exp_df[1]
        df['YeuCauKinhNghiemTB_clean'] = df[['Exp_Min_clean', 'Exp_Max_clean']].mean(axis=1)
        def label_exp(val):
            if pd.isna(val) or val == 0: return "Không yêu cầu"
            if val < 1: return "Dưới 1 năm"
            if 1 <= val < 3: return "1 - 3 năm"
            if 3 <= val < 5: return "3 - 5 năm"
            return "Trên 5 năm"
        df['PhanLoaiKinhNghiem'] = df['YeuCauKinhNghiemTB_clean'].apply(label_exp)
        df['KinhNghiem_clean'] = df.apply(lambda x: self._make_exp_clean({
            'KinhNghiemToiThieu': x['Exp_Min_clean'], 
            'KinhNghiemToiDa': x['Exp_Max_clean']
        }), axis=1)

        # --- 2. XỬ LÝ LƯƠNG ---
        # 1. Trích xuất dữ liệu thô
        salary_res = df['MucLuong'].apply(self.process_interest_text).apply(pd.Series)

        # 2. Đảm bảo lấy đúng các cột và điền 0 vào giá trị NaN để tránh lỗi tính toán
        df['MucLuongMin_clean'] = pd.to_numeric(salary_res['MucLuongMin'], errors='coerce').fillna(0)
        df['MucLuongMax_clean'] = pd.to_numeric(salary_res['MucLuongMax'], errors='coerce').fillna(0)
        df['MucLuongTB_clean'] = pd.to_numeric(salary_res['MucLuongTB'], errors='coerce').fillna(0)
        df['KhoangLuong'] = salary_res['KhoangLuong'].fillna("Thỏa thuận")
        df['Luong_clean'] = df.apply(lambda x: self._make_salary_clean({
        'MucLuongToiThieu': x['MucLuongMin_clean'], 
        'MucLuongToiDa': x['MucLuongMax_clean']
        }), axis=1)

        # --- 3. XỬ LÝ QUY MÔ CÔNG TY ---
        source_col = 'QuyMoCongTy'

        if source_col in df.columns:
           
            size_df = df[source_col].apply(self._extract_size_numerics)
            df['QuyMoMin_clean'] = size_df[0]
            df['QuyMoMax_clean'] = size_df[1]
            df['QuyMoTB_clean'] = df[['QuyMoMin_clean', 'QuyMoMax_clean']].mean(axis=1)
            df['QuyMo_clean'] = df.apply(lambda x: self._make_size_clean({
                'QuyMoMin': x['QuyMoMin_clean'], 
                'QuyMoMax': x['QuyMoMax_clean']
            }), axis=1)
            df['LoaiQuyMo_clean'] = df['QuyMo_clean'].apply(self._binning_quy_mo)
            
        else:
            # Trường hợp không có cột dữ liệu gốc để tránh lỗi code
            for col in [source_col, 'QuyMo_clean', 'LoaiQuyMo_clean']: 
                df[col] = "Không xác định"
            for col in ['QuyMoMin_clean', 'QuyMoMax_clean', 'QuyMoTB_clean']: 
                df[col] = np.nan
        df['CongTy_clean'] = df['CongTy'].astype(str).str.title()
        df['CongViec_clean'] = df['CongViec'].apply(self.clean_title)
        df['CapBac_clean'] = df.apply(lambda row: self._extract_rank_strict(row['CongViec'], row['YeuCauKinhNghiemTB_clean']), axis=1)
        df['SoLuongTuyen_clean'] = df.apply(self.clean_quantity, axis=1)
        df['HocVan_clean'] = df.apply(self.find_education_coalesced, axis=1)
 
        # Hình thức & WorkMode
        df['HinhThuc_clean'] = df.apply(self._determine_employment_type, axis=1)
        df['NoiLamViec_clean'] = df.apply(self._determine_work_mode, axis=1)     
        # Kỹ năng
        df['Hard_Skills'] = df['YeuCauUngVien'].apply(lambda x: self._extract_skills(x, 'hard'))
        df['Soft_Skills'] = df['YeuCauUngVien'].apply(lambda x: self._extract_skills(x, 'soft'))
        df['Ngoai_Ngu'] = df['YeuCauUngVien'].apply(lambda x: self._extract_skills(x, 'language'))
        df['LinhVuc_Web'] = df['LinhVuc'].apply(self.clean_industry_macro)
        df['LinhVuc_Con'] = df['CongViec'].apply(self.clean_industry)
        df['ViTri_Tuyen_Clean'] = df['CongViec'].apply(self.clean_job_role)
        # --- 5. ĐỊA ĐIỂM (Explode) ---
        df_final = self.clean_location_data(df)
        # Deadline & Meta
        df_final['HanNopHoSo_clean'] = df_final.apply(self.clean_deadline, axis=1)
        df_final['NgayXuLyDL'] = datetime.now() 
        # Tạo JobHash 
        df_final['JobHash'] = df_final.apply(lambda x: hashlib.md5(f"{x['LinkBaiTuyenDung']}{x['Tinh_Thanh']}".encode()).hexdigest(), axis=1)
        # 5. Mapping đầy đủ tất cả các cột (Gốc + Clean)
        column_mapping = {
    'LinkBaiTuyenDung': 'LinkBaiTuyenDung',
    'CongTy': 'CongTy',
    'CongViec': 'CongViec',
    'CapBac': 'CapBac',
    'ViTri': 'ViTri',
    'MucLuong': 'MucLuong',
    'MoTaCongViec': 'MoTaCongViec',
    'YeuCauUngVien': 'YeuCauUngVien',
    'YeuCauKinhNghiem': 'YeuCauKinhNghiem',
    'LinhVuc': 'LinhVuc',
    'HocVan': 'HocVan',
    'HinhThucLamViec': 'HinhThucLamViec',
    'SoLuongTuyen': 'SoLuongTuyen',
    'QuyMoCongTy': 'QuyMoCongTy',
    'HanNopHoSo': 'HanNopHoSo',
    'Nguon': 'Nguon',
    'NgayCaoDuLieu': 'NgayCaoDuLieu',
    'JobHash': 'JobHash',
    'CongTy_clean': 'CongTy_clean',
    'CongViec_clean': 'CongViec_clean',
    'CapBac_clean': 'CapBac_clean',
    'HocVan_clean': 'HocVan_clean',
    'HinhThuc_clean': 'HinhThuc_clean', # Full-time/Part-time
    'NoiLamViec_clean': 'WorkMode_clean', # Remote/Onsite/Hybrid

    
    'Tinh_Thanh': 'Tinh_Thanh',
    'KhuVuc': 'KhuVuc',
    'Latitude': 'Latitude',
    'Longitude': 'Longitude',
   
    'Luong_clean': 'Luong_clean',            
    'MucLuongMin_clean': 'MucLuongMin_clean',
    'MucLuongTB_clean': 'MucLuongTB_clean',
    'MucLuongMax_clean': 'MucLuongMax_clean',
    'KhoangLuong': 'KhoangLuong',             

    
    'KinhNghiem_clean': 'KinhNghiem_clean',   
    'Exp_Min_clean': 'Exp_Min_clean',
    'Exp_Max_clean': 'Exp_Max_clean',
    'PhanLoaiKinhNghiem': 'PhanLoaiKinhNghiem',

    
    'QuyMoCongTy': 'QuyMoCongTy',           
    'QuyMoMin_clean': 'QuyMoMin_clean',     
    'QuyMoMax_clean': 'QuyMoMax_clean',     
    'QuyMoTB_clean': 'QuyMoTB_clean',       
    'QuyMo_clean': 'QuyMo_clean',           
    'LoaiQuyMo_clean': 'LoaiQuyMo_clean',   
    
    'Hard_Skills': 'Hard_Skills',
    'Soft_Skills': 'Soft_Skills',
    'Ngoai_Ngu': 'Ngoai_Ngu',
    'LinhVuc_Web':'LinhVuc_Web',
    'LinhVuc_Con':'LinhVuc_Con',
    'ViTri_Tuyen_Clean': 'ViTri_Tuyen_Clean',
    'SoLuongTuyen_clean': 'SoLuongTuyen_clean',
    'HanNopHoSo_clean': 'HanNopHoSo_clean',
    'NgayXuLyDL': 'NgayXuLyDL'

        }
        valid_cols = [c for c in column_mapping.keys() if c in df_final.columns]
        df_ready = df_final[valid_cols].copy()
        df_ready.rename(columns=column_mapping, inplace=True)
        print(f"[3/3] Đang đẩy {len(df_ready)} dòng lên Database...")
        self.save_data_directly(df_ready)
        print(" HOÀN TẤT!")
if __name__ == "__main__":
    if DATABASE_URL:
        pipeline = RecruitmentETL(DATABASE_URL)
        pipeline.run()
    else:
        print("Không tìm thấy Connection String.")