from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
import time
import getpass

driver = webdriver.Chrome()

# Tạo url
url = 'https://www.reddit.com/login/'

# Truy cập
driver.get(url)
time.sleep(2)

# Nhap thong tin nguoi dung
my_email = "11a4.phanxuanduong@gmail.com"
my_password = "Duongtrum1"


actionChains = ActionChains(driver)
sleep = True
while sleep:
    if input("enter: ") == 'Y':
        break

for i in range(7):
    actionChains.key_down(Keys.TAB).perform()
    
actionChains.send_keys(my_email).perform()
actionChains.key_down(Keys.TAB).perform()

actionChains.send_keys(my_password + Keys.ENTER).perform()

for i in range(3):
    actionChains.key_down(Keys.TAB).perform()

actionChains.key_down(Keys.ENTER).perform()
time.sleep(5)



# Truy cap trang post bai
url2 = 'https://www.reddit.com/user/tungit2024/submit/?type=TEXT'
driver.get(url2)
time.sleep(2)

for i in range(17):
    actionChains.key_down(Keys.TAB).perform()


actionChains.send_keys('Vi du post').perform()


actionChains.key_down(Keys.TAB)
actionChains.key_down(Keys.TAB).perform()

actionChains.send_keys('Le Nhat Tung').perform()

for i in range(2):
    actionChains.key_down(Keys.TAB).perform()
    time.sleep(3)

actionChains.send_keys(Keys.ENTER).perform()
driver.quit()





