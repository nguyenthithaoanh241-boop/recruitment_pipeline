# pipeline/transformer.py

import pandas as pd
from sqlalchemy import create_engine
from .config import DATABASE_URL
import re

def transform_data():
    """
    Lấy dữ liệu từ bảng staging, làm sạch, biến đổi và nạp vào bảng production.
    """
    engine = create_engine(DATABASE_URL)
    print("🚀 Bắt đầu quá trình Transform...")

    try:
        # Đọc toàn bộ dữ liệu từ bảng thô
        df_raw = pd.read_sql("SELECT * FROM staging.raw_jobs", engine)
        
        if df_raw.empty:
            print("✅ Không có dữ liệu thô để transform.")
            return

        print(f"    -> Đã đọc {len(df_raw)} dòng từ staging.raw_jobs.")

        # ----- BẮT ĐẦU CÁC BƯỚC TRANSFORM -----

        # 1. Xóa các dòng trùng lặp dựa trên link công việc
        df_transformed = df_raw.drop_duplicates(subset=['link'], keep='last')

        # 2. Xử lý cột lương (Salary) - Đây là một ví dụ phức tạp
        def parse_salary(salary_str):
            if not isinstance(salary_str, str):
                return None, None, None
            
            salary_str = salary_str.lower()
            if 'thỏa thuận' in salary_str or 'cạnh tranh' in salary_str:
                return 0, 0, 'Thỏa thuận'
            
            # Tìm các số trong chuỗi (kể cả số thập phân)
            numbers = [float(s) for s in re.findall(r'-?\d+\.?\d*', salary_str.replace(',', ''))]
            
            # Xác định đơn vị tiền tệ
            currency = 'VND'
            multiplier = 1_000_000 # Mặc định là triệu VND
            if '$' in salary_str or 'usd' in salary_str:
                currency = 'USD'
                multiplier = 1
            
            if len(numbers) == 2:
                return numbers[0] * multiplier, numbers[1] * multiplier, currency
            elif len(numbers) == 1:
                if 'trên' in salary_str or 'từ' in salary_str:
                    return numbers[0] * multiplier, None, currency
                if 'lên đến' in salary_str or 'tối đa' in salary_str:
                    return None, numbers[0] * multiplier, currency
                return numbers[0] * multiplier, numbers[0] * multiplier, currency
            
            return None, None, None

        salaries = df_transformed['salary'].apply(parse_salary)
        df_transformed[['salary_min', 'salary_max', 'currency']] = pd.DataFrame(salaries.tolist(), index=df_transformed.index)

        # 3. Xử lý ngày tháng (Dates)
        df_transformed['post_date'] = pd.to_datetime(df_transformed['post_date'], errors='coerce')
        df_transformed['deadline'] = pd.to_datetime(df_transformed['deadline'], errors='coerce')

        # 4. Chuẩn hóa địa điểm (Location)
        df_transformed['location'] = df_transformed['work_location'].str.strip()

        # 5. Xử lý Skills -> chuyển thành mảng
        df_transformed['skills'] = df_transformed['skills'].str.split(',').apply(
            lambda x: [skill.strip() for skill in x] if isinstance(x, list) else None
        )

        # 6. Trích xuất số năm kinh nghiệm
        def parse_experience(exp_str):
            if not isinstance(exp_str, str) or 'không yêu cầu' in exp_str.lower():
                return 0
            numbers = [int(s) for s in re.findall(r'\d+', exp_str)]
            return min(numbers) if numbers else None
            
        df_transformed['experience_years_min'] = df_transformed['experience'].apply(parse_experience)


        # ----- KẾT THÚC TRANSFORM -----

        # Chọn và đổi tên các cột để khớp với bảng production
        df_final = df_transformed[[
            'job_id', 'title', 'company', 'salary_min', 'salary_max', 'currency', 
            'location', 'experience_years_min', 'level', 'skills', 
            'post_date', 'deadline', 'source_web', 'link'
        ]]

        print(f"    -> Transform hoàn tất. Sẵn sàng ghi {len(df_final)} dòng vào production.")

        # Ghi dữ liệu đã làm sạch vào bảng production
        # 'replace' sẽ xóa bảng cũ và tạo lại với dữ liệu mới.
        df_final.to_sql(
            name='clean_jobs',
            con=engine,
            schema='production',
            if_exists='replace',
            index=False
        )
        print("✅ Dữ liệu đã được làm sạch và lưu vào bảng production.clean_jobs.")

    except Exception as e:
        print(f"❌ Lỗi trong quá trình transform: {e}")

if __name__ == '__main__':
    transform_data()