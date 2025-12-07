import sys
import os
import pandas as pd
import numpy as np
import re
import sqlalchemy
import hashlib
from datetime import datetime, timedelta
from sqlalchemy import text

# ==============================================================================
# 1. CẤU HÌNH & IMPORT TỪ CONFIG
# ==============================================================================
# Thiết lập đường dẫn để import được 'pipeline.config'
current_dir = os.getcwd()

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
    
        self.merge_map = self._init_merge_mapping()
        self.coord_map = self._init_full_coords()
        self.industry_map = self._init_industry_map()
        #self.job_title_map = self._init_job_title_map()
        self.skill_map = self._init_skill_map()
        self.edu_map = self._init_education()

    # --------------------------------------------------------------------------
    # A. TỪ ĐIỂN LUẬT GỘP (INPUT -> TỈNH ĐÍCH)
    # --------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self.garbage_locations = {
            '10 nơi khác', 
            'toàn quốc', 
            'tỉnh khác', 
            'nhiều nơi',
            'việc làm khác' # Ví dụ thêm
        }
    def save_data_via_procedure(self, df, chunk_size=1000):
        """
        Đẩy dữ liệu vào SQL Server thông qua Stored Procedure sp_Import_FactCleanJobs_JSON
        """
        if df.empty:
            print("⚠️ DataFrame rỗng, không có dữ liệu để lưu.")
            return
        df = df.where(pd.notnull(df), None)

        # Đảm bảo các cột ngày tháng là string hoặc datetime object chuẩn
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                 # Format về YYYY-MM-DD để SQL Server dễ nuốt
                 df[col] = df[col].astype(str).replace('NaT', None)

        total_rows = len(df)
        print(f"Đang đẩy {total_rows} dòng vào SQL Server (Upsert)...")

        # 2. Mở kết nối Transaction (tự động Commit/Rollback)
        try:
            with self.engine.begin() as conn: 
                for start in range(0, total_rows, chunk_size):
                    end = start + chunk_size
                    chunk = df.iloc[start:end]
                    
                    # 3. Convert DataFrame -> JSON String
                    # orient='records': Tạo list of dicts [{}, {}]
                    # date_format='iso': Chuẩn ngày tháng quốc tế
                    # force_ascii=False: Giữ nguyên tiếng Việt có dấu
                    json_data = chunk.to_json(orient='records', date_format='iso', force_ascii=False)
                    
                    try:
                        # 4. Thực thi Stored Procedure (SQL Server Syntax)
                        # Lưu ý: Phải dùng :json_param làm placeholder để tránh lỗi SQL Injection và lỗi ký tự đặc biệt
                        # Thêm [dbo]. để SQL Server không phải đoán mò
                        query = text("EXEC [dbo].[sp_Import_FactCleanJobs_JSON] @JsonData = :json_param")
                        
                        conn.execute(query, {"json_param": json_data})
                        
                        print(f"    Chunk {start} -> {min(end, total_rows)}: Thành công.")
                        
                    except Exception as e:
                        print(f"   Lỗi tại chunk {start}: {e}")
                        # Debug: In ra một phần JSON để kiểm tra lỗi cú pháp nếu có
                        print(f"   --> Sample Data gây lỗi: {json_data[:200]}...")
                        raise e 
            
            print("🎉 Hoàn tất quá trình Import dữ liệu!")
            
        except Exception as e:
            print(f"⛔ Lỗi nghiêm trọng trong quá trình lưu DB: {e}")
            # Re-raise để dừng chương trình nếu cần
            raise e
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
    def _init_education(self):
        return {
            'Tiến sĩ': ['tiến sĩ', 'doctorate', 'phd'],
            'Thạc sĩ': ['thạc sĩ', 'master'],
            'Đại học': ['đại học', 'cử nhân', 'kỹ sư', 'bachelor', 'university'],
            'Cao đẳng': ['cao đẳng', 'college'],
            'Trung cấp': ['trung cấp', 'intermediate'],
            'Tốt nghiệp phổ thông': ['tốt nghiệp thpt', 'cấp 3', 'high school', '12/12']
        }

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

    # ==========================================================================
    # C. CÁC HÀM XỬ LÝ (TRANSFORMATION)
    # ==========================================================================
    def process_interest_text(self, text):
        """
        Hàm chính: Nhận vào text (ví dụ cột Salary hoặc Quyền lợi) -> Trả về thông tin lương
        """
        # 1. Khởi tạo giá trị mặc định
        result = {
            "MucLuongMin": np.nan,
            "MucLuongMax": np.nan,
            "MucLuongTB": np.nan,
            "KhoangLuong": "Thỏa thuận" # Label phân loại
        }

        if not isinstance(text, str) or not text:
            return result

        text_lower = text.lower()

        # 2. Trích xuất Lương (Giả sử bạn đã có hàm self.clean_salary trả về Min/Max)
     
        salary_min, salary_max = self.clean_salary(text_lower)

        # 3. Tính Lương Trung Bình (Logic xử lý Null)
        salary_tb = np.nan
        
        if pd.notna(salary_min) and pd.notna(salary_max):
            salary_tb = (salary_min + salary_max) / 2
        elif pd.notna(salary_min): # Chỉ có min (VD: "Từ 10 triệu")
            salary_tb = salary_min
        elif pd.notna(salary_max): # Chỉ có max (VD: "Lên đến 20 triệu")
            salary_tb = salary_max

        # 4. Gán giá trị tính toán vào kết quả
        result["MucLuongMin"] = salary_min
        result["MucLuongMax"] = salary_max
        result["MucLuongTB"] = salary_tb

        # 5. Phân loại khoảng lương (Labeling)
        # Gọi hàm phụ đã tách ra ở trên
        result["KhoangLuong"] = self._get_salary_range_label(salary_tb)

        return result
    
    def process_requirements_text(self, text):
        """
        Hàm tổng: Nhận vào text 'Yêu Cầu Ứng Viên' -> Trả về Dict các thông tin trích xuất
        """
        if not isinstance(text, str) or not text:
            # Trả về giá trị mặc định nếu text rỗng
            return {
                "YeuCauKinhNghiemMin": np.nan,
                "YeuCauKinhNghiemMax": np.nan,
                "YeuCauKinhNghiemTB": np.nan,
                "PhanLoaiKinhNghiem": "Không yêu cầu kinh nghiệm",
                "HardSkills": "Không yêu cầu",
                "SoftSkills": "Không yêu cầu",
                "HocVan_YeuCau": "Khác",
                "CapBac_YeuCau": "Nhân viên", 
                "LinhVuc_YeuCau": "Công nghệ & Viễn thông"
            }
        
        text_lower = text.lower()

        # 1. Trích xuất Kinh Nghiệm (Số năm)
        exp_min, exp_max = self._extract_experience_numerics_strict(text_lower)
        # Tính trung bình và phân loại
        exp_tb = 0.0
        label = "Không yêu cầu kinh nghiệm"
        
        if pd.notna(exp_max):
            exp_tb = (exp_min + exp_max) / 2 if pd.notna(exp_min) else exp_max
        elif pd.notna(exp_min):
            exp_tb = exp_min
        
        if exp_tb > 0:
            if exp_tb < 1: label = "Dưới 1 năm"
            elif exp_tb < 3: label = "1 – 3 năm"
            elif exp_tb < 5: label = "3 – 5 năm"
            else: label = "Trên 5 năm"

        # 2. Trích xuất Kỹ năng (Cứng/Mềm)
        hard_skills = self._extract_hard_skills(text) 
        soft_skills = self._extract_soft_skills(text) 

        # 3. Trích xuất Học vấn
        hoc_van = self.clean_education(text_lower)

        # 4. Trích xuất Cấp bậc (Dựa trên yêu cầu)
        cap_bac = self._extract_rank_strict(text_lower)
        #5, trích xuất lĩnh vực
        linh_vuc = self.clean_industry(text_lower)

        return {
            "YeuCauKinhNghiemMin": exp_min,
            "YeuCauKinhNghiemMax": exp_max,
            "YeuCauKinhNghiemTB": exp_tb,
            "PhanLoaiKinhNghiem": label,
            "HardSkills": hard_skills,
            "SoftSkills": soft_skills,
            "HocVan_YeuCau": hoc_van,
            "CapBac_YeuCau": cap_bac,
            "LinhVuc_YeuCau":linh_vuc
        }



    def _extract_experience_numerics_strict(self, text):
        # Logic tách số năm kinh nghiệm từ text
        if any(kw in text for kw in ['không yêu cầu', 'no experience', 'chưa có']): 
            return 0.0, 0.0
        
        matches = re.findall(r'(\d+(?:\.\d+)?)\s*(?:năm|year)', text)
        nums = [float(n) for n in matches]
        
        if not nums: return np.nan, np.nan
        
        # Logic min/max đơn giản
        if len(nums) == 1: 
            val = nums[0]
            if "trên" in text or "hơn" in text: return val, np.nan
            if "dưới" in text: return 0.0, val
            return val, val
        
        return min(nums), max(nums)

    def _extract_rank_strict(self, text):
        # Tìm cấp bậc yêu cầu trong text (ví dụ: "yêu cầu trình độ Senior")
        text = str(text).lower()
        if any(k in text for k in ['thực tập', 'intern', 'trainee']): return "Thực tập sinh"
        if any(k in text for k in ['phó giám đốc', 'phó gđ', 'vp ']): return "Phó giám đốc"
        if any(k in text for k in ['giám đốc', 'gđ', 'director', 'ceo']): return "Giám đốc"
        if any(k in text for k in ['trưởng phòng', 'manager', 'lead', 'trưởng nhóm']): return "Trưởng phòng"
        return "Nhân viên"



    # ==========================================================================
    # NHÓM 2: CÁC HÀM XỬ LÝ TRƯỜNG "MÔ TẢ CÔNG VIỆC"
    # (Working Time, Work Mode, Employment Type)
    # ==========================================================================

    def process_description_text(self, text):
        """
        Hàm tổng: Nhận vào text 'Mô Tả Công Việc' -> Trả về Dict thông tin môi trường làm việc
        """
        if not isinstance(text, str) or not text:
            return {
                "KieuLamViec": "Onsite",
                "HinhThucLamViec_clean": "Full-time",
               
            }
        
        text_lower = text.lower()
        # 1. Kiểu làm việc (Remote/Hybrid)
        mode_work = self._determine_work_mode(text_lower) 

        # 2. Hình thức (Full/Part)
        type_work = self._determine_employment_type(text_lower) 
        #3. Số lượng tuyển
        
        return {
            "KieuLamViec": mode_work,
            "HinhThucLamViec_clean": type_work,
        }
    # 1. Hàm quan trọng nhất: Xử lý Địa điểm (Gộp Tỉnh + Giữ tọa độ gốc)

    def clean_location_data(self, df):
        print("--- Đang xử lý Location (Lọc Rác Sớm và Chuyển về Tỉnh chuẩn) ---")

        # -------------------------------------------------------------
        # 🎯 BƯỚC 0: LỌC RÁC SỚM VÀ CHUẨN BỊ (NEW STEP)
        # -------------------------------------------------------------
        
        # Chuẩn hóa giá trị rác về chữ thường
        garbage_lower = [g.lower() for g in self.garbage_locations]
        # Hàm kiểm tra và thay thế giá trị rác bằng NaN (để bị loại khi explode)
        def clean_raw_location(location):
            if not isinstance(location, str):
                return location
            
            loc_lower = location.lower().strip()
            
            # Kiểm tra khớp chính xác hoặc khớp một phần với giá trị rác
            if loc_lower in garbage_lower or any(g in loc_lower for g in garbage_lower):
                return np.nan # Thay thế bằng NaN để loại bỏ sau này
            
            return location
            
        df['ViTri'] = df['ViTri'].apply(clean_raw_location)
        
        # 1. TÁCH DÒNG (Explode)
        # df.explode() sẽ tự động loại bỏ các giá trị NaN/None sau khi áp dụng .apply() ở trên
        df['Temp_Loc_List'] = df['ViTri'].astype(str).apply(
            # Đảm bảo str('nan') cũng được loại trừ
            lambda x: [i.strip() for i in re.split(r'[;,|&]|\s+-\s+', x) if i.strip() and i.strip().lower() != 'nan']
        )
        df_exploded = df.explode('Temp_Loc_List').dropna(subset=['Temp_Loc_List']) # Loại bỏ các dòng chỉ chứa NaN
        
        # 2. HÀM MAPPING (Định nghĩa logic) - Hàm này được đơn giản hóa vì giá trị rác đã bị lọc
        def get_geo_info(loc_raw):
            # [vi_tri_clean, tinh_thanh_chuan, region, lat, lng]
            
            # Nếu loc_raw là NaN (từ bước explode), hãy bỏ qua
            if pd.isna(loc_raw) or not isinstance(loc_raw, str):
                # Các giá trị rác đã bị lọc, nên nếu gặp non-string/NaN ở đây thì có thể là dữ liệu rỗng.
                return ["Khác", "Khác", "Khác", None, None]

            # [A] Chuẩn hóa Input
            loc_lower = loc_raw.lower().strip()
            loc_clean = re.sub(r'^(tp\.?|t\.|tỉnh|thành phố)\s+', '', loc_lower).strip()
            
            region, lat, lng = "Khác", None, None
            tinh_thanh_chuan = "Khác"
            
            # -------------------------------------------------------------
            # 🎯 BƯỚC B: Tìm Tỉnh Chuẩn (Tinh_Thanh) bằng Merge Map
            # -------------------------------------------------------------
            
            # Tìm khớp chính xác
            if loc_clean in self.merge_map:
                tinh_thanh_chuan = self.merge_map[loc_clean]
            else:
                # Tìm khớp một phần (VD: 'phường đình bảng, bắc ninh' -> 'bắc ninh')
                for k, v in self.merge_map.items():
                    if k in loc_clean: 
                        tinh_thanh_chuan = v
                        break
            
            # -------------------------------------------------------------
            # 🎯 BƯỚC C: Tìm Tọa Độ và XÁC ĐỊNH ViTri_clean
            # -------------------------------------------------------------
            
            vi_tri_clean = tinh_thanh_chuan # Mặc định là tên tỉnh đã chuẩn hóa
            
            # 1. Nếu vị trí có trong Coord Map (chi tiết và có tọa độ)
            if loc_clean in self.coord_map:
                info = self.coord_map[loc_clean]
                region, lat, lng, ten_chuan_coord = info[0], info[1], info[2], info[3]
                vi_tri_clean = ten_chuan_coord # Gán lại ViTri_clean là tên chi tiết
                
            # 2. Xử lý trường hợp không tìm thấy (tinh_thanh_chuan vẫn là "Khác")
            if tinh_thanh_chuan == "Khác":
                vi_tri_clean = "Khác"
                
            # -------------------------------------------------------------
            
            return [vi_tri_clean, tinh_thanh_chuan, region, lat, lng]
            
        # 3. ÁP DỤNG LOGIC VÀO DATAFRAME
        df_exploded['Temp_Geo_List'] = df_exploded['Temp_Loc_List'].apply(get_geo_info)
        
        # 4. TÁCH CỘT
        df_exploded['ViTri_clean'] = df_exploded['Temp_Geo_List'].apply(lambda x: x[0])
        df_exploded['Tinh_Thanh'] = df_exploded['Temp_Geo_List'].apply(lambda x: x[1])
        df_exploded['KhuVuc'] = df_exploded['Temp_Geo_List'].apply(lambda x: x[2])
        
        df_exploded['Latitude'] = df_exploded['Temp_Geo_List'].apply(lambda x: float(x[3]) if x[3] is not None else None).round(6)
        df_exploded['Longitude'] = df_exploded['Temp_Geo_List'].apply(lambda x: float(x[4]) if x[4] is not None else None).round(6)
        
        # Xóa cột tạm
        df_exploded.drop(columns=['Temp_Loc_List', 'Temp_Geo_List'], inplace=True)
        
        return df_exploded
    def clean_title(self, text):
        if not isinstance(text, str): 
            return None 
        
        # 1. Chuyển về chữ thường để xử lý logic xóa từ rác
        text = str(text).lower()

        # --- DANH SÁCH TỪ RÁC (Giữ nguyên logic của bạn) ---
        noise_patterns = [
            r'tuyển\s*gấp', r'cần\s*tuyển', r'tuyển\s*dụng', r'tuyển', 
            r'urgent', r'hot', r'gấp', r'đi\s*làm\s*ngay',
            r'vị\s*trí',
            r'số\s*lượng\s*\d+', r'\d+\s*slots?',
            r'lương\s*.*', 
            r'thu\s*nhập.*',
            r'\d+\s*-\s*\d+\s*(triệu|tr|m|usd|\$)',
            r'upto\s*\d+',
            r'\d+\s*(năm|tháng)\s*k(?:inh)?\s*n(?:ghiệm)?', 
            r'k(?:inh)?\s*n(?:ghiệm)?\s*.*', 
            r'full\s*time', r'part\s*time', r'fulltime', r'parttime',
            r'remote', r'onsite', r'hybrid', r'wfh',
            r'tại\s*văn\s*phòng', r'work\s*from\s*home'
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, ' ', text)

        # --- XÓA ĐỊA ĐIỂM (Giữ nguyên logic của bạn) ---
        if hasattr(self, 'location_map_values'):
            for loc in self.location_map_values:
                loc_clean = loc.lower().replace('.', r'\.')
                text = re.sub(r'(?:tại|ở|khu\s*vực|tp\.?)\s*' + re.escape(loc_clean) + r'\b', ' ', text)
                text = re.sub(r'\b' + re.escape(loc_clean) + r'\b', ' ', text)

        # --- CHUẨN HÓA LẠI FORMAT (SỬA Ở ĐÂY) ---
        
        text = re.sub(r'[^\w\s\-\+#\./&]', ' ', text)
        
        # Xóa khoảng trắng thừa
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Xóa ký tự đặc biệt ở đầu cuối
        text = text.strip('.|,&-')

        if not text: 
            return None
        
        # Theo ảnh mẫu output mong muốn của bạn thì có vẻ bạn cần .title()
        return text.title()
    def clean_salary(self, raw_salary):
        """
        Hàm chuẩn hóa lương: Trả về [Min, Max]
        Input: "Up to 1000 USD", "10 - 20 Triệu", "Thỏa thuận"
        Output: (0.0, 25000000.0)
        """
        # 1. Xử lý null/rỗng
        if not isinstance(raw_salary, str) or not raw_salary: 
            return 0.0, 0.0 # Trả về Tuple hoặc List
        
        text = raw_salary.lower()
        
        # 2. Xác định Đơn vị (Unit) - Sửa lỗi logic if
        unit = 1
        if "usd" in text or "$" in text: 
            unit = 25000
        elif any(x in text for x in ["triệu", "tr", "millions", "m", "trieu"]): 
            unit = 1000000
        elif any(x in text for x in ["nghìn", "k", "nghin"]): 
            unit = 1000
            
        # 3. Làm sạch số (Giữ lại dấu chấm thập phân, xóa dấu phẩy hàng nghìn)
        # Ví dụ: "1,000.5" -> "1000.5"
        text_clean = text.replace(',', '')
        
        # Regex bắt số thực (float)
        matches = re.findall(r'\d+(?:\.\d+)?', text_clean)
        try:
            nums = [float(n) for n in matches]
        except:
            return 0.0, 0.0
        
        if not nums: return 0.0, 0.0
        
        # 4. Logic phân chia Min/Max
        min_sal, max_sal = 0.0, 0.0
        
        if len(nums) == 1:
            val = nums[0] * unit
            # Logic ngữ cảnh
            if any(kw in text for kw in ["đến", "tới", "up to", "dưới", "max"]): 
                min_sal, max_sal = 0.0, val
            elif any(kw in text for kw in ["từ", "trên", "hơn", "min", "from"]): 
                min_sal, max_sal = val, 0.0 # 0.0 ở max nghĩa là Open-ended (Không giới hạn)
            else: 
                # Trường hợp chỉ ghi "1000$" -> Coi là lương cứng
                min_sal = max_sal = val
                
        elif len(nums) >= 2:
            # Trường hợp "10 - 20 triệu"
            v1 = nums[0] * unit
            v2 = nums[1] * unit
            # Sắp xếp lại để đảm bảo min luôn nhỏ hơn max
            min_sal, max_sal = min(v1, v2), max(v1, v2)

        return min_sal, max_sal
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
            return "9999-12-31"

    def clean_experience(self, text):
        text = str(text).lower()
        if 'không' in text: return pd.Series([0.0, 0.0, 0.0])
        nums = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', text)]
        if not nums: return pd.Series([None, None, None])
        mi, ma = (nums[0], nums[0]) if len(nums)==1 else (min(nums), max(nums))
        return pd.Series([mi, (mi+ma)/2, ma])

    # --- [2] HÀM MỚI: XỬ LÝ KINH NGHIỆM (Logic gộp cột + Parse) ---
    def _parse_experience_to_list(self, row):
        # Hàm con để parse text sang số
        def parse_text_exp(text):
            if not isinstance(text, str) or not text: return None
            t = text.lower().strip()
            if any(kw in t for kw in ['không yêu cầu', 'no experience', 'chưa có']): return [0.0, 0.0]
            nums = [float(n) for n in re.findall(r'\d+(?:\.\d+)?', t)]
            if not nums: return None
            if 'tháng' in t and 'năm' not in t: nums = [n / 12 for n in nums]
            
            mi, ma = None, None
            if 'dưới 1 năm' in t: mi, ma = 0.0, 1.0
            elif any(kw in t for kw in ['trên', 'hơn', 'over']): mi, ma = nums[0], None
            elif any(kw in t for kw in ['dưới', 'less']): mi, ma = 0.0, nums[0]
            elif len(nums) >= 2: nums.sort(); mi, ma = nums[0], nums[-1]
            elif len(nums) == 1: mi, ma = nums[0], nums[0]
            return [mi, ma]

        # Ưu tiên cột chính
        res = parse_text_exp(row.get('YeuCauKinhNghiem'))
        if res is not None: return res
        
        # Fallback sang cột phụ
        res = parse_text_exp(row.get('YeuCauUngVien'))
        if res is not None: return res
        
        return [np.nan, np.nan]

    # --- [3] HÀM MỚI: XỬ LÝ QUY MÔ (Trả về List thuần) ---
    def _parse_size_to_list(self, text):
        if not isinstance(text, str) or not text: return [np.nan, np.nan]
        clean_text = text.lower().replace('.', '').replace(',', '')
        nums = [float(n) for n in re.findall(r'\d+', clean_text)]
        if not nums: return [np.nan, np.nan]
        
        if any(kw in clean_text for kw in ['dưới', 'ít hơn']): return [0.0, nums[0]]
        if any(kw in clean_text for kw in ['trên', 'hơn']): return [nums[0], np.nan]
        if len(nums) >= 2: 
            nums.sort()
            return [nums[0], nums[-1]]
        return [nums[0], nums[0]]
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
    # --- HÀM MỚI 1: XÁC ĐỊNH HÌNH THỨC (Full-time/Part-time) ---
    def _determine_employment_type(self, text):
        if not isinstance(text, str): return "Full-time"
        
        # Logic ưu tiên từ khóa
        kw_freelance = ['freelance', 'freelancer', 'tự do', 'cộng tác viên', 'ctv', 'project base', 'theo dự án', 'thời vụ']
        if any(k in text for k in kw_freelance):
            return "Freelance"
            
        kw_parttime = ['part time', 'part-time', 'bán thời gian', 'ca gãy', '4 tiếng', 'parttime']
        if any(k in text for k in kw_parttime):
            return "Part-time"
            
        # Mặc định là Full-time
        return "Full-time"

    # --- HÀM MỚI 2: XÁC ĐỊNH KIỂU LÀM VIỆC (Onsite/Remote) ---
    def _determine_work_mode(self, text):
        if not isinstance(text, str): return "Onsite"
        
        kw_hybrid = [
            'hybrid', 'linh hoạt', 'xen kẽ', 'flexible', 'kết hợp', 'mix', 
            'bán từ xa', 'semi-remote', 'ngày lên văn phòng', 'days at office'
        ]
        if any(k in text for k in kw_hybrid):
            return "Hybrid"
            
        kw_remote = ['remote', 'từ xa', 'wfh', 'work from home', 'tại nhà', 'không cần lên văn phòng']
        if any(k in text for k in kw_remote):
            return "Remote"
            
        # Mặc định là Onsite
        return "Onsite"
    
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
        return "Công nghệ & Viễn thông"

    # 4. Xử lý Số lượng tuyển (Quantity) - ĐÃ CẬP NHẬT
    def clean_quantity(self, row):
        DEFAULT_QTY = 1
        qty_from_col = DEFAULT_QTY
        raw_col = str(row.get('SoLuongTuyen')) if pd.notna(row.get('SoLuongTuyen')) else ""
        raw_col_lower = raw_col.lower()
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
    def clean_education(self, text):
        if not isinstance(text, str):
            return "Không yêu cầu"
        
        t = text.lower()
        
        # Duyệt qua Dictionary để tìm từ khóa
        # Lưu ý: Logic này sẽ trả về kết quả đầu tiên tìm thấy. 
        # Ví dụ dưới đây đang ưu tiên theo thứ tự trong Dict:
        for level, keywords in self.edu_map.items():
            if any(k in t for k in keywords):
                return level
                
        return "Không yêu cầu"

    def find_education_coalesced(self, row):
        primary_edu = row.get('HocVan')
        secondary_req = row.get('YeuCauUngVien')

        # Hàm phụ trợ để kiểm tra chuỗi hợp lệ
        def is_valid_string(text):
            return pd.notna(text) and isinstance(text, str) and text.strip()

        # 1. Kiểm tra cột chính (HocVan)
        # Nếu cột này đã chuẩn, ta lấy luôn. Nếu chưa chuẩn, bạn có thể gọi self.clean_education(primary_edu)
        if is_valid_string(primary_edu):
            return primary_edu  # Hoặc: return self.clean_education(primary_edu)

        # 2. Kiểm tra cột phụ (YeuCauUngVien)
        # QUAN TRỌNG: Phải dùng clean_education để "bóc tách" từ khóa
        if is_valid_string(secondary_req):
            extracted_edu = self.clean_education(secondary_req)
            
            # Chỉ trả về nếu tìm thấy bằng cấp (khác "Không yêu cầu")
            # Nếu clean_education trả về "Không yêu cầu", ta để code chạy xuống dưới
            if extracted_edu != "Không yêu cầu":
                return extracted_edu

        # 3. Mặc định
        return "Không yêu cầu"
    
    # 6. Xử lý Kỹ năng (Skills)
    def _extract_hard_skills(self, text):
        if not isinstance(text, str): return ""
        h = []
        # Quét Hard Skills
        for k, keywords in self.skill_map.get('hard', {}).items():
            for kw in keywords:
                # Dùng regex boundary để tránh bắt nhầm từ con
                if re.search(r'(?:^|\W)(' + kw + r')(?:$|\W)', text):
                    h.append(k)
                    break 
        return ", ".join(sorted(h))

    # --- HÀM MỚI 2: TÁCH KỸ NĂNG MỀM (Soft Skills) ---
    def _extract_soft_skills(self, text):
        if not isinstance(text, str): return ""
        s = []
        # Quét Soft Skills
        for k, keywords in self.skill_map.get('soft', {}).items():
            for kw in keywords:
                if re.search(r'(?:^|\W)(' + kw + r')(?:$|\W)', text):
                    s.append(k)
                    break
        return ", ".join(sorted(s))

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
        print("⏳ [1/7] Tải dữ liệu từ Fact_JobPostings...")
        # Test limit
        df = pd.read_sql("SELECT*FROM fact_jobpostings limit 5;", self.engine)
        
        if df.empty:
            print("⚠️ Không có dữ liệu mới.")
            return None
        
        print("⏳ [2/7] Chuẩn hóa Text cơ bản & Xử lý Ngày tháng...")
        
        # 1. Chuẩn hóa Text cơ bản
        df['CongTy_clean'] = df['CongTy'].astype(str).str.strip().str.capitalize()
        df['CongViec_clean'] = df['CongViec'].apply(self.clean_title)
        
        # 2. Xử lý Ngày tháng
        df['NgayCaoDuLieu'] = pd.to_datetime(df['NgayCaoDuLieu'], errors='coerce').dt.date
        df['HanNopHoSo_clean'] = df.apply(self.clean_deadline, axis=1)
        df['HanNopHoSo_clean'] = pd.to_datetime(df['HanNopHoSo_clean'], errors='coerce').dt.date

        print("⏳ [3/7] Áp dụng Logic Trích xuất (Feature Extraction)...")

        # --- A. XỬ LÝ LƯƠNG ---
        print("   -> Đang xử lý Lương...")
        # Hàm trả về Dict, tách thành cột
        salary_info = df['MucLuong'].apply(self.process_interest_text).apply(pd.Series)
        df = pd.concat([df, salary_info], axis=1)

        # --- B. XỬ LÝ YÊU CẦU ---
        print("   -> Đang xử lý Yêu cầu (KN, Kỹ năng, Học vấn)...")
        df['Temp_Full_Req'] = df['YeuCauUngVien'].fillna('').astype(str) + " " + df['YeuCauKinhNghiem'].fillna('').astype(str)
        req_info = df['Temp_Full_Req'].apply(self.process_requirements_text).apply(pd.Series)
        
        # [FIX QUAN TRỌNG 1]: Xóa cột trùng trước khi concat
        cols_to_drop = [col for col in ['HocVan', 'LinhVuc_clean'] if col in req_info.columns]
        req_info = req_info.drop(columns=cols_to_drop)
        df = pd.concat([df, req_info], axis=1)
        
        # [Override Logic]
        df['HocVan_clean'] = df.apply(lambda row: self.find_education_coalesced(row) if pd.notna(row['HocVan']) else row['HocVan'], axis=1)
        df['LinhVuc_clean'] = df['LinhVuc'].apply(self.clean_industry)

        # --- C. XỬ LÝ MÔ TẢ ---
        print("   -> Đang xử lý Mô tả (Working Mode, Type)...")
        # Hàm process_description_text (đã sửa ở Bước 1) không còn trả về SoLuongTuyen nữa
        desc_info = df['MoTaCongViec'].apply(self.process_description_text).apply(pd.Series)
        df = pd.concat([df, desc_info], axis=1)
        
        # [FIX QUAN TRỌNG 2]: Gọi clean_quantity riêng biệt, truyền vào axis=1 (Row)
        print("   -> Đang tính toán Số lượng tuyển...")
        df['SoLuongTuyen_clean'] = df.apply(self.clean_quantity, axis=1)

        # --- D. XỬ LÝ QUY MÔ ---
        print("   -> Đang xử lý Quy mô công ty...")
        df['Temp_Size_List'] = df['QuyMoCongTy'].apply(self._parse_size_to_list)
        df['QuyMoCongTyMin_clean'] = df['Temp_Size_List'].apply(lambda x: x[0])
        df['QuyMoCongTyMax_clean'] = df['Temp_Size_List'].apply(lambda x: x[1])
        
        def calc_avg_size(row):
            mi, ma = row['QuyMoCongTyMin_clean'], row['QuyMoCongTyMax_clean']
            if pd.isna(mi): return np.nan
            if pd.isna(ma): return mi
            return (mi + ma) / 2
        
        df['QuyMoCongTyTB_clean'] = df.apply(calc_avg_size, axis=1)
        
        size_bins = [0, 10, 100, 500, 1000, 5000, float('inf')]
        size_labels = ["Dưới 10 nhân viên", "10 - 100 nhân viên", "100 - 500 nhân viên", 
                       "500 - 1000 nhân viên", "1000 - 5000 nhân viên", "Trên 5000 nhân viên"]
        df['PhanLoaiQuyMoCongTy'] = pd.cut(df['QuyMoCongTyTB_clean'], bins=size_bins, labels=size_labels, right=False)
        df['PhanLoaiQuyMoCongTy'] = df['PhanLoaiQuyMoCongTy'].astype(str).replace({'nan': 'Không xác định', 'None': 'Không xác định'})

        print("⏳ [4/7] Điền khuyết dữ liệu (Imputation)...")
        cols_sal = ['MucLuongMin', 'MucLuongMax']
        for col in cols_sal:
            if col in df.columns:
                df[col] = df[col].replace(0, np.nan)
                df[col] = df[col].fillna(
                    df.groupby(['CongViec_clean', 'CapBac_YeuCau'])[col].transform('mean')
                )
                df[col] = df[col].fillna(0)

        df['MucLuongTB'] = (df['MucLuongMin'] + df['MucLuongMax']) / 2
        df['KhoangLuong'] = df['MucLuongTB'].apply(self._get_salary_range_label)

        print("⏳ [5/7] Tách địa điểm (Explode)...")
        df_final = self.clean_location_data(df)
        
        print("⏳ [6/7] Tạo JobHash & Chọn cột Output...")
        
        # --- [CẬP NHẬT LOGIC HASH THEO YÊU CẦU] ---
        def _make_hash(row):
            link = str(row.get('LinkBaiTuyenDung', '')).strip().lower()
            
            # [SỬA TẠI ĐÂY]: Dùng ViTri_clean thay vì Tinh_Thanh
            # ViTri_clean là giá trị unique sau khi explode (VD: dòng 1 là HN, dòng 2 là HCM)
            vitri = str(row.get('ViTri_clean', '')).strip().lower() 
            
            title = str(row.get('CongViec', '')).strip().lower()
            
            # Tạo key duy nhất: Link + Địa điểm cụ thể + Tên Job
            combined = f"{link}|{vitri}|{title}"
            return hashlib.md5(combined.encode('utf-8')).hexdigest()

        # Áp dụng hàm Hash
        df_final['JobHash'] = df_final.apply(_make_hash, axis=1)
        df_final['NgayXuLyDL'] = datetime.now()

        output_cols = [
            'JobID', 'JobHash', 'LinkBaiTuyenDung', 
            'CongTy', 'CongTy_clean', 
            'CongViec', 'CongViec_clean', 
            'CapBac', 'CapBac_YeuCau', 
            'ViTri', 'ViTri_clean', 'Tinh_Thanh', 'KhuVuc', 'Latitude', 'Longitude',
            'MucLuong', 'MucLuongMin', 'MucLuongMax', 'MucLuongTB', 'KhoangLuong',
            'MoTaCongViec', 'YeuCauUngVien',
            'YeuCauKinhNghiem', 'YeuCauKinhNghiemMin', 'YeuCauKinhNghiemMax', 'YeuCauKinhNghiemTB', 'PhanLoaiKinhNghiem',
            'HardSkills', 'SoftSkills',
            'LinhVuc', 'LinhVuc_clean',
            'HocVan', 'HocVan_clean',
            'HinhThucLamViec', 'HinhThucLamViec_clean', 'KieuLamViec_clean', # Map đúng tên cột
            'SoLuongTuyen', 'SoLuongTuyen_clean',
            'QuyMoCongTy', 'QuyMoCongTyMin_clean', 'QuyMoCongTyMax_clean', 'QuyMoCongTyTB_clean', 'PhanLoaiQuyMoCongTy',
            'HanNopHoSo', 'HanNopHoSo_clean',
            'Nguon', 'NgayCaoDuLieu', 'NgayXuLyDL'
        ]

        # Map tên cột (nếu tên trong process_* khác tên trong output)
        if 'KieuLamViec' in df_final.columns: df_final['KieuLamViec_clean'] = df_final['KieuLamViec']
        if 'HinhThucLamViec' in df_final.columns: df_final['HinhThucLamViec_clean'] = df_final['HinhThucLamViec']
        if 'YeuCauKiNangCung_clean' in df_final.columns: df_final['HardSkills'] = df_final['YeuCauKiNangCung_clean']
        if 'YeuCauKiNangMem_clean' in df_final.columns: df_final['SoftSkills'] = df_final['YeuCauKiNangMem_clean']

        # Fill cột thiếu
        for col in output_cols:
            if col not in df_final.columns: df_final[col] = None
            
        df_ready = df_final[output_cols].copy()

        print("⏳ [7/7] Lưu vào Database (Procedure JSON Upsert)...")
        self.save_data_via_procedure(df_ready)
        return df_final
if __name__ == "__main__":
    pipeline = RecruitmentETL(DATABASE_URL)
    df = pipeline.run()
    