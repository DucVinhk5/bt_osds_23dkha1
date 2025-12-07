import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re
import os # Thêm thư viện để kiểm tra/xóa file DB (tùy chọn)

######################################################
## I. Cấu hình và Chuẩn bị
######################################################

# Thiết lập tên file DB và Bảng
DB_FILE = 'Painters_Data.db'
TABLE_NAME = 'painters_info'

# Tùy chọn cho Chrome (có thể chạy ẩn nếu cần, nhưng để dễ debug thì không dùng)
# chrome_options = Options()
# chrome_options.add_argument("--headless") 

# Nếu muốn bắt đầu với DB trống, có thể xóa file cũ (Tùy chọn)
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Đã xóa file DB cũ: {DB_FILE}")

# Mở kết nối SQLite và tạo bảng nếu chưa tồn tại
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Tạo bảng
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    name TEXT PRIMARY KEY, -- Sử dụng tên làm khóa chính để tránh trùng lặp
    birth TEXT,
    death TEXT,
    nationality TEXT
);
"""
cursor.execute(create_table_sql)
conn.commit()
print(f"Đã kết nối và chuẩn bị bảng '{TABLE_NAME}' trong '{DB_FILE}'.")

# Hàm đóng driver an toàn
def safe_quit_driver(driver):
    try:
        if driver:
            driver.quit()
    except Exception as e:
        print(f"Error: {e}")
        pass

######################################################
## II. Lấy Đường dẫn (URLs)
######################################################

print("\n--- Bắt đầu Lấy Đường dẫn ---")

url = "https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22F%22"

driver = webdriver.Chrome()
driver.get(url)

ul_tag = driver.find_element(By.XPATH, "(//ul)[20]")

links = [a_tag.get_attribute("href") for a_tag in ul_tag.find_elements(By.XPATH, ".//li/a")]


######################################################
## III. Lấy thông tin & LƯU TRỮ TỨC THỜI
######################################################

print("\n--- Bắt đầu Cào và Lưu Trữ Tức thời ---")
count = 0
for link in links:
    # Giới hạn số lượng truy cập để thử nghiệm nhanh
    if (count >= 5): # Đã tăng lên 5 họa sĩ để có thêm dữ liệu mẫu
        break
    count = count + 1
    try:
        driver.get(link)
        time.sleep(2)

        # 1. Lấy tên họa sĩ
        try:
            name = driver.find_element(By.TAG_NAME, "h1").text
        except:
            name = ""
        
        # 2. Lấy ngày sinh (Born)
        try:
            birth_element = driver.find_element(By.XPATH, "//th[text()='Born']/following-sibling::td")
            birth = birth_element.text
            # Trích xuất định dạng ngày (ví dụ: 12 June 1900)
            birth_match = re.findall(r'[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}', birth)
            birth = birth_match[0] if birth_match else ""
        except:
            birth = ""
            
        # 3. Lấy ngày mất (Died)
        try:
            death_element = driver.find_element(By.XPATH, "//th[text()='Died']/following-sibling::td")
            death = death_element.text
            death_match = re.findall(r'[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}', death)
            death = death_match[0] if death_match else ""
        except:
            death = ""
            
        # 4. Lấy quốc tịch (Nationality)
        try:
            nationality_element = driver.find_element(By.XPATH, "//th[text()='Born']/following-sibling::td")
            nationality_element = nationality_element.find_element(By.CSS_SELECTOR, "div.birthplace")
            nationality = nationality_element.find_elements(By.TAG_NAME, 'a')[1].text
        except:
            nationality = ""
        
        # 5. LƯU TỨC THỜI VÀO SQLITE
        insert_sql = f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (name, birth, death, nationality) 
        VALUES (?, ?, ?, ?);
        """
        # Sử dụng 'INSERT OR IGNORE' để bỏ qua nếu Tên (PRIMARY KEY) đã tồn tại
        cursor.execute(insert_sql, (name, birth, death, nationality))
        conn.commit()
        print(f"  --> Đã lưu thành công: {name}")

    except Exception as e:
        print(f"Lỗi khi xử lý hoặc lưu họa sĩ {link}: {e}")
        safe_quit_driver(driver)

safe_quit_driver(driver)
        
print("\nHoàn tất quá trình cào và lưu dữ liệu tức thời.")

######################################################
## IV. Truy vấn SQL Mẫu và Đóng kết nối
######################################################


# A. Yêu Cầu Thống Kê và Toàn Cục
# 1. Đếm tổng số họa sĩ đã được lưu trữ trong bảng.
query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
cursor.execute(query)
count = cursor.fetchone()[0]

print(f"\nSố bản ghi trong {TABLE_NAME}: {count}")
# 2. Hiển thị 5 dòng dữ liệu đầu tiên để kiểm tra cấu trúc và nội dung bảng.
print("\n5 dòng dữ liệu đầu tiên:\n")
query = f"SELECT * FROM {TABLE_NAME} LIMIT 5"
cursor.execute(query)
rows = cursor.fetchall()

print(f"{'Name':<20} | {'Born':<10} | {'Death':<10} | {'Nationality':<20}")
for row in rows:
    result = f"{row[0]:<20} | {row[1]:<10} | {row[2]:<10} | {row[3]:<20}"
    print('_' * len(result))
    print(result)

# 3. Liệt kê danh sách các quốc tịch duy nhất có trong tập dữ liệu.
query = f"SELECT DISTINCT nationality FROM {TABLE_NAME}"
cursor.execute(query)
nationalities = cursor.fetchall()

print(f"\nCác quốc gia duy nhất trong {TABLE_NAME}:")
for nationality in nationalities:
    print('-', nationality)

# B. Yêu Cầu Lọc và Tìm Kiếm
# 4. Tìm và hiển thị tên của các họa sĩ có tên bắt đầu bằng ký tự 'F'.
query = f"SELECT name from {TABLE_NAME}"
cursor.execute(query)
names = cursor.fetchall()

print(f"\nCác họa sĩ có tên bắt đầu bằng ký tự 'F' trong {TABLE_NAME}:")
for name in names:
    print('-', name)

# 5. Tìm và hiển thị tên và quốc tịch của những họa sĩ có quốc tịch chứa từ khóa 'French' (ví dụ: French, French-American).
query = f"SELECT name, nationality from {TABLE_NAME} WHERE nationality LIKE '%French%'"
cursor.execute(query)
rows = cursor.fetchall()

print(f"\nCác họa sĩ có quốc tịch chứa từ khóa 'French' trong {TABLE_NAME}:")
for row in rows:
    print('-', row)

# 6. Hiển thị tên của các họa sĩ không có thông tin quốc tịch (hoặc để trống, hoặc NULL).
query = f"SELECT name from {TABLE_NAME} WHERE nationality = '' OR nationality IS NULL"
cursor.execute(query)
names = cursor.fetchall()

print(f"\nCác họa sĩ có không có thông tin quốc tịch trong {TABLE_NAME}:")
for name in names:
    print('-', name)
# 7. Tìm và hiển thị tên của những họa sĩ có cả thông tin ngày sinh và ngày mất (không rỗng).
query = f"""
SELECT name from {TABLE_NAME} 
WHERE 
    (birth IS NOT NULL AND birth != '')
    AND
    (death IS NOT NULL AND death != '')"""
cursor.execute(query)
names = cursor.fetchall()

print(f"\nTên của những họa sĩ có cả thông tin ngày sinh và ngày mất (không rỗng) trong {TABLE_NAME}:")
for name in names:
    print('-', name)
# 8. Hiển thị tất cả thông tin của họa sĩ có tên chứa từ khóa '%Fales%' (ví dụ: George Fales Baker).
query = f"SELECT name from {TABLE_NAME} WHERE name LIKE '%Fales%'"
cursor.execute(query)
names = cursor.fetchall()

print(f"\nCác họa sĩ có tên chứa từ khóa 'Fales' trong {TABLE_NAME}:")
for name in names:
    print('-', name)
# C. Yêu Cầu Nhóm và Sắp Xếp
# 9. Sắp xếp và hiển thị tên của tất cả họa sĩ theo thứ tự bảng chữ cái (A-Z).
query = f"""
    SELECT name
    FROM {TABLE_NAME}
    ORDER BY name"""
cursor.execute(query)
names = cursor.fetchall()

print(f"\nTên của tất cả họa sĩ theo thứ tự bảng chữ cái (A-Z) trong {TABLE_NAME}:")
for name in names:
    print('-', name)
# 10. Nhóm và đếm số lượng họa sĩ theo từng quốc tịch.
query = f"""
    SELECT nationality, COUNT(name)
    FROM {TABLE_NAME}
    GROUP BY nationality"""
cursor.execute(query)
rows = cursor.fetchall()

for nationality, count in rows:
    print(f"Quốc tịch: {nationality:<30} | Số lượng: {count}")
# Đóng kết nối cuối cùng
conn.commit()
conn.close()
print("\nĐã đóng kết nối cơ sở dữ liệu.")
