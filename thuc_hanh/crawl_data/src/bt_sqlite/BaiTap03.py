# Đề Bài Thực Hành: Cào Dữ Liệu Long Châu và Quản Lý SQLite
# I. Mục Tiêu
#     Thực hiện cào dữ liệu sản phẩm từ trang web chính thức của chuỗi nhà thuốc Long Châu bằng công cụ Selenium, lưu trữ dữ liệu thu thập được một cách tức thời vào cơ sở dữ liệu SQLite, và kiểm tra chất lượng dữ liệu.

# II. Yêu Cầu Kỹ Thuật (Scraping & Lưu trữ)
#     Công cụ: Sử dụng thư viện Selenium kết hợp với Python và Pandas (cho việc quản lý DataFrame tạm thời và lưu vào DB).

#     Phạm vi Cào: Chọn một danh mục sản phẩm cụ thể trên trang Long Châu (ví dụ: "Thực phẩm chức năng", "Chăm sóc da", hoặc "Thuốc") và cào ít nhất 50 sản phẩm (có thể cào nhiều trang/URL khác nhau).

#     Dữ liệu cần cào: Đối với mỗi sản phẩm, cần thu thập ít nhất các thông tin sau (table phải có các cột bên dưới):

#         Mã sản phẩm (id): cố gắng phân tích và lấy mã sản phẩm gốc từ trang web, nếu không được thì dùng mã tự tăng.

#         Tên sản phẩm (product_name)

#         Giá bán (price)

#         Giá gốc/Giá niêm yết (nếu có, original_price)

#         Đơn vị tính (ví dụ: Hộp, Chai, Vỉ, unit)

#         Link URL sản phẩm (product_url) (Dùng làm định danh duy nhất)

#     Lưu trữ Tức thời:

#         Sử dụng thư viện sqlite3 để tạo cơ sở dữ liệu (longchau_db.sqlite).

#         Thực hiện lưu trữ dữ liệu ngay lập tức sau khi cào xong thông tin của mỗi sản phẩm (sử dụng conn.cursor().execute() hoặc DataFrame.to_sql(if_exists='append')) thay vì lưu trữ toàn bộ sau khi kết thúc quá trình cào.

#         Sử dụng product_url hoặc một trường định danh khác làm PRIMARY KEY (hoặc kết hợp với lệnh INSERT OR IGNORE) để tránh ghi đè nếu chạy lại code.

# III. Yêu Cầu Phân Tích Dữ Liệu (Query/Truy Vấn)
#     Sau khi dữ liệu được thu thập, tạo và thực thi ít nhất 15 câu lệnh SQL (queries) để khảo sát chất lượng và nội dung dữ liệu.

#     Nhóm 1: Kiểm Tra Chất Lượng Dữ Liệu (Bắt buộc)
#         Kiểm tra trùng lặp (Duplicate Check): Kiểm tra và hiển thị tất cả các bản ghi có sự trùng lặp dựa trên trường product_url hoặc product_name.

#         Kiểm tra dữ liệu thiếu (Missing Data): Đếm số lượng sản phẩm không có thông tin Giá bán (price là NULL hoặc 0).

#         Kiểm tra giá: Tìm và hiển thị các sản phẩm có Giá bán lớn hơn Giá gốc/Giá niêm yết (logic bất thường).

#         Kiểm tra định dạng: Liệt kê các unit (đơn vị tính) duy nhất để kiểm tra sự nhất quán trong dữ liệu.

#         Tổng số lượng bản ghi: Đếm tổng số sản phẩm đã được cào.

#     Nhóm 2: Khảo sát và Phân Tích (Bổ sung)
#         Sản phẩm có giảm giá: Hiển thị 10 sản phẩm có mức giá giảm (chênh lệch giữa original_price và price) lớn nhất.

#         Sản phẩm đắt nhất: Tìm và hiển thị sản phẩm có giá bán cao nhất.

#         Thống kê theo đơn vị: Đếm số lượng sản phẩm theo từng Đơn vị tính (unit).

#         Sản phẩm cụ thể: Tìm kiếm và hiển thị tất cả thông tin của các sản phẩm có tên chứa từ khóa "Vitamin C".

#         Lọc theo giá: Liệt kê các sản phẩm có giá bán nằm trong khoảng từ 100.000 VNĐ đến 200.000 VNĐ.

#     Nhóm 3: Các Truy vấn Nâng cao (Tùy chọn)
#         Sắp xếp: Sắp xếp tất cả sản phẩm theo Giá bán từ thấp đến cao.

#         Phần trăm giảm giá: Tính phần trăm giảm giá cho mỗi sản phẩm và hiển thị 5 sản phẩm có phần trăm giảm giá cao nhất (Yêu cầu tính toán trong query hoặc sau khi lấy data).

#         Xóa bản ghi trùng lặp: Viết câu lệnh SQL để xóa các bản ghi bị trùng lặp, chỉ giữ lại một bản ghi (sử dụng Subquery hoặc Common Table Expression - CTE).

#         Phân tích nhóm giá: Đếm số lượng sản phẩm trong từng nhóm giá (ví dụ: dưới 50k, 50k-100k, trên 100k).

#         URL không hợp lệ: Liệt kê các bản ghi mà trường product_url bị NULL hoặc rỗng.
import time
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd

# ==========================================
# 1. Setup Selenium
# ==========================================
chrome_options = Options()
chrome_options.add_argument("--headless=new")
driver = webdriver.Chrome(options=chrome_options)

url = "https://nhathuoclongchau.com.vn/thuc-pham-chuc-nang/vitamin-khoang-chat"
driver.get(url)

# ==========================================
# 2. Setup SQLite
# ==========================================
DB_FILE = 'longchau_db.sqlite'

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Đã xóa file DB cũ: {DB_FILE}")

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT,
    price INTEGER,
    unit TEXT,
    original_price INTEGER,
    product_url TEXT UNIQUE
)
""")

conn.commit()

# ==========================================
# 3. Hàm lưu dữ liệu tức thời
# ==========================================
def save_product(data):
    cursor.execute("""
        INSERT OR IGNORE INTO products (product_name, price, unit, original_price, product_url)
        VALUES (?, ?, ?, ?, ?)
    """, data)
    conn.commit()

# ==========================================
# 4. Bắt đầu cào dữ liệu
# ==========================================
while True:
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "button.mt-3")
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(1)
    except:
        break

for product in driver.find_elements(By.CSS_SELECTOR, "div.px-4 > div.grid > div.h-full"):
    # Lấy tên sản phẩm với xử lý lỗi
    try:
        product_name = product.find_element(By.CSS_SELECTOR, "h3.overflow-hidden").text
        product_name = product_name if product_name else None
    except:
        product_name = None

    # Lấy giá sản phẩm và loại bỏ ký tự không cần thiết
    try:
        price = product.find_element(By.CSS_SELECTOR, "div.text-blue-5 > span.font-semibold").text
        price = price.replace(".", "").replace("đ", "").strip()
    except:
        price = None

    # Lấy đơn vị giá, đảm bảo có ít nhất 2 phần tử sau khi split
    try:
        unit = product.find_element(By.CSS_SELECTOR, "div.text-blue-5 > span.text-label2").text.split()
        unit = unit[1] if len(unit) > 1 else None
    except:
        unit = None

    # Lấy giá gốc và xử lý ký tự không cần thiết
    try:
        original_price = product.find_element(By.CSS_SELECTOR, "div.font-normal.text-gray-6").text
        original_price = original_price.replace(".", "").replace("đ", "").strip()
    except:
        original_price = price

    # Lấy URL sản phẩm
    try:
        product_url = product.find_element(By.CSS_SELECTOR, "a.block.px-3").get_attribute("href")
    except:
        product_url = None

    save_product([product_name, price, unit, original_price, product_url])

driver.quit()
print("Save Success!")

conn.close()

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

print("\n===== NHÓM 1: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU =====")

# 1. Kiểm tra trùng lặp theo product_url
print("\n1. Trùng lặp product_url:")
cursor.execute("""
SELECT product_url, COUNT(*)
FROM products
GROUP BY product_url
HAVING COUNT(*) > 1
""")
print(cursor.fetchall())

# 2. Kiểm tra trùng lặp theo product_name
print("\n2. Trùng lặp product_name:")
cursor.execute("""
SELECT product_name, COUNT(*)
FROM products
GROUP BY product_name
HAVING COUNT(*) > 1
""")
print(cursor.fetchall())

# 3. Dữ liệu thiếu giá
print("\n3. Sản phẩm không có giá:")
cursor.execute("SELECT COUNT(*) FROM products WHERE price IS NULL OR price = 0")
print(cursor.fetchone()[0])

# 4. Liệt kê unit duy nhất
print("\n4. Danh sách đơn vị tính:")
cursor.execute("SELECT DISTINCT unit FROM products")
print(cursor.fetchall())

# 5. Tổng số bản ghi
print("\n5. Tổng số lượng sản phẩm:")
cursor.execute("SELECT COUNT(*) FROM products")
print(cursor.fetchone()[0])


print("\n===== NHÓM 2: KHẢO SÁT VÀ PHÂN TÍCH =====")

# 6. 10 sản phẩm giá cao nhất
print("\n6. 10 sản phẩm đắt nhất:")
cursor.execute("""
SELECT product_name, price FROM products 
ORDER BY price DESC LIMIT 10
""")
print(cursor.fetchall())

# 7. Sản phẩm có giá cao nhất
print("\n7. Sản phẩm giá cao nhất:")
cursor.execute("""
SELECT product_name, price FROM products 
ORDER BY price DESC LIMIT 1
""")
print(cursor.fetchone())

# 8. Đếm số lượng theo đơn vị
print("\n8. Số lượng theo đơn vị:")
cursor.execute("""
SELECT unit, COUNT(*) FROM products 
GROUP BY unit
""")
print(cursor.fetchall())

# 9. Tìm sản phẩm chứa từ 'Vitamin C'
print("\n9. Sản phẩm chứa 'Vitamin C':")
cursor.execute("""
SELECT * FROM products 
WHERE product_name LIKE '%Vitamin C%'
""")
print(cursor.fetchall())

# 10. Lọc theo khoảng giá
print("\n10. Sản phẩm từ 100k - 200k:")
cursor.execute("""
SELECT product_name, price FROM products 
WHERE price BETWEEN 100000 AND 200000
""")
print(cursor.fetchall())


print("\n===== NHÓM 3: NÂNG CAO =====")

# 11. Sắp xếp theo giá tăng dần
print("\n11. Sắp xếp theo giá tăng dần:")
cursor.execute("""
SELECT product_name, price FROM products 
ORDER BY price ASC
""")
print(cursor.fetchall())

# 12. Gom nhóm theo mức giá
print("\n12. Thống kê theo nhóm giá:")
cursor.execute("""
SELECT
    CASE 
        WHEN price < 50000 THEN 'Dưới 50k'
        WHEN price BETWEEN 50000 AND 100000 THEN '50k - 100k'
        ELSE 'Trên 100k'
    END AS price_group,
    COUNT(*)
FROM products
GROUP BY price_group
""")
print(cursor.fetchall())

# 13. URL rỗng hoặc NULL
print("\n13. URL không hợp lệ:")
cursor.execute("""
SELECT * FROM products WHERE product_url IS NULL OR product_url = ''
""")
print(cursor.fetchall())

# 14. Xóa bản ghi trùng lặp (giữ id nhỏ nhất)
print("\n14. Xóa bản ghi trùng lặp theo product_name (mô phỏng):")
cursor.execute("""
DELETE FROM products
WHERE id NOT IN (
    SELECT MIN(id)
    FROM products
    GROUP BY product_name
)
""")
conn.commit()
print("Đã xóa bản ghi trùng lặp.")

# 15. Kiểm tra lại số lượng sau khi xóa
print("\n15. Tổng số bản ghi sau khi làm sạch:")
cursor.execute("SELECT COUNT(*) FROM products")
print(cursor.fetchone()[0])

conn.close()

