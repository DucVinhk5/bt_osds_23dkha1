from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd

#Tạo tuy chọn
options = Options()
#Thiết lập firefox chỉ hiển thị giao diện
options.headless = False

#Khởi tạo driver
driver = webdriver.Firefox(options=options)

url = "https://gochek.vn/collections/all"
driver.get(url)
time.sleep(3)


# Tìm phần tử body của trang để gửi phím mũi tên xuống
body = driver.find_element(By.TAG_NAME, "body")
time.sleep(3)

for k in range (10):
    try:
        # Lấy tất cả các button trên trang
       buttons = driver.find_elements(By.TAG_NAME, "button")

       # Duyệt qua từng button
       for button in buttons:
           # Kiểm tra nếu nội dung của button chứa "Xem thêm" và "sản phẩm"
           if "Xem thêm" in button.text and "sản phẩm" in button.text:
               # Di chuyển tới button và click
               button.click()
               break  # Thoát khỏi vòng lặp nếu đã click thành công
    
    except Exception as e:
        print(f"Lỗi: {e}")

# Nhấn phím mũi tên xuống nhiều lần để cuộn xuống từ từ
for i in range(30):  # Lặp 30 lần, mỗi lần cuộn xuống một ít
    body.send_keys(Keys.ARROW_DOWN)
    time.sleep(0.2)  # Tạm dừng 0.2 giây giữa mỗi lần cuộn để trang tải nội dung

# Tạm dừng thêm vài giây để trang tải hết nội dung ở cuối trang
time.sleep(1)

# Tao cac list
stt = []
ten_san_pham = []
gia_ban = []
hinh_anh = []

# Tìm tất cả các button có nội dung là "Chọn mua"
buttons = driver.find_elements(By.XPATH, "//button[text()='Chọn mua']")

print(len(buttons))

# Lấy tất cả card sản phẩm
products = driver.find_elements(
    By.CSS_SELECTOR,
    "div.col-md-3.col-sm-4.col-xs-6.pro-loop.col_fix20"
)

print("Số sản phẩm:", len(products))
stt = []
ten = []
gia_moi = []
gia_cu = []
link_sp = []
hinh_anh = []
# lay tung san pham
for i, p in enumerate(products, start=1):

    # Tên + link
    try:
        name_el = p.find_element(By.CSS_SELECTOR, "h3.pro-name a")
        name = name_el.text.strip()
        href = name_el.get_attribute("href")
    except:
        name = ""
        href = ""

# Giá
    try:
        try:
            price_block = p.find_element(By.CSS_SELECTOR, "p.pro-price.highlight")
        except:
            price_block = p.find_element(By.CSS_SELECTOR, "p.pro-price")

        price_new = ""
        price_old = ""

        spans = price_block.find_elements(By.TAG_NAME, "span")
        for s in spans:
            classes = s.get_attribute("class") or ""
            text = s.text.strip()
            if not text:
                continue
            if "pro-price-del" in classes:
                price_old = text
            else:
                if not price_new:
                    price_new = text

    except Exception as e:
        price_new = ""
        price_old = ""


    # Ảnh
    try:
        img = p.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
    except:
        img = ""

    if name:
        stt.append(i)
        ten.append(name)
        gia_moi.append(price_new)
        gia_cu.append(price_old)
        link_sp.append(href)
        hinh_anh.append(img)


# Tạo df
df = pd.DataFrame({
    "STT": stt,
    "Tên sản phẩm": ten,
    "Giá mới": gia_moi,
    "Giá cũ": gia_cu,
    "Link sản phẩm": link_sp,
    "Hình ảnh": hinh_anh
})

df.to_excel("danh_sach_sp_EX02c.xlsx", index=False)


