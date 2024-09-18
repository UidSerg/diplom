#!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime
#import csv
import pandas as pd
import pymysql
import re

# Create a connection to the MySQL database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='airflow',
    db='airflow',
    port=33061
)
cur = conn.cursor()

def sroll_to(driver):
    """Фунция скроллинга до конца"""
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight)")
        time.sleep(3)
        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def contains_all_words(text1, text2):
    """Фунция проверки соответсвия выдачи запросу"""
    # Разбиваем тексты на слова
    words1 = set(text1.lower().split())
    text2 = re.sub(r'[^\w\s]', '', text2)
    words2 = set(text2.lower().split())
    
    # Проверяем, все ли слова из text1 есть в text2
    return words1.issubset(words2)


# Настройка веб-драйвера (например, Chrome)
user_agent = (
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    '(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
)
chrome_option = Options()
chrome_option.add_argument("--disable-blink-features=AutomationControlled") ## скрыть использование webdriver
chrome_option.add_argument("--no-sandbox")  # Отключает режим sandbox
chrome_option.add_argument("--disable-dev-shm-usage")  # Отключает использование /dev/shm в Chrome
chrome_option.add_argument(f'user-agent={user_agent}')
# Укажите путь к ChromeDriver
service = Service(executable_path='/usr/local/bin/chromedriver')

# Создайте экземпляр WebDriver
driver = webdriver.Chrome(service=service, options=chrome_option)


try:
    # Открываем ozon.ru
    driver.get("https://www.wildberries.ru/")
    time.sleep(10)  # Ожидание загрузки страницы

    # Находим строку поиска
    search_field = driver.find_element(By.ID, "searchInput")

    # Вводим в строку поиска "что ищем"
    search_field.clear() # очистим если сторока не пустая
    search_text = "ssd 128гб"
    search_field.send_keys(search_text)

    # Нажимаем Enter для поиска
    search_field.send_keys(Keys.RETURN)
    time.sleep(10)  # Ожидание загрузки результатов поиска

    # ищем меню сортировки и открываем
    sort = driver.find_element(By.XPATH,"//button[text()='По популярности']")
    sort.click()
    time.sleep(5)  # Ожидание загрузки 
    
    # сортируем по "По возрастанию цены"
    #price_low = driver.find_element(By.XPATH,"//span[@class='radio-with-text__text'][text()='По убыванию цены']")
    price_low = driver.find_element(By.XPATH,"//li[text()='По возрастанию цены']")
    price_low.click()

    time.sleep(10)  # Ожидание загрузки результатов поиска
    # скролл вниз 
    sroll_to(driver)
    time.sleep(5)  # Ожидание загрузки результатов поиска

    
    # получаем список элементов
    results = driver.find_elements(By.XPATH, "//div[@class='product-card__wrapper']")
    time.sleep(5)  # Ожидание загрузки результатов поиска
    
    data = []
    for result in results:
        try:
            title = result.find_element(By.XPATH, "./a").get_attribute("aria-label")         
        except Exception as e:
            print(f"ОШИБКА!!! {e}")
            title = "Not Found 404"
            
        try:
            price = result.find_element(By.XPATH, ".//p/span/ins").text
            price = price[:-2].strip().replace(' ', '')              
        except Exception as e:
            print(f"ОШИБКА!!! {e}")
            price = "Not Found 404"

        data.append({
            'title': title,
            'price': price,
            })
    
    """
    with open('./home_work_7/results_wb.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    """

    # Показываем результаты поиска 
    print("Результаты поиска загружены. Браузер будет закрыт через 10 секунд.")
    time.sleep(10)

    df = pd.DataFrame(data)
    # фильтрация 
    t1 = search_text.split(' ')[0]
    t2 = search_text.split(' ')[1]
    filtered_df = df[df['title'].str.contains(t1, case=False, na=False)].copy()
    filtered_df.loc[:,'site'] = 'wb'
    print(filtered_df.head(10)) 
        
    # Запись данных в таблицу
    for index, row in filtered_df.iterrows():
        cur.execute("""
            INSERT INTO price (data, title, price, site)
            VALUES (%s, %s, %s, %s);
        """, (datetime.now(), row['title'], row['price'], row['site']))

    # Сохранение изменений
    conn.commit()
    # Закрытие курсора и соединения
    cur.close()
    conn.close()

except Exception as e:
    print(f"Ошибка: {e}")

finally:
    driver.quit()