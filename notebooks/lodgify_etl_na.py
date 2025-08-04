# Import from libraries
import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Selenium Configuration and Scraping
def configure_driver():
    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def handle_popups(driver):
    time.sleep(5)
    try:
        reject_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Reject')]")
        reject_btn.click()
        print("Rejected cookies.")
        time.sleep(2)
    except:
        print("No reject cookie button found.")

    try:
        dismiss_btn = driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Dismiss sign-in info."]')
        dismiss_btn.click()
        print("Dismissed sign-in popup.")
        time.sleep(2)
    except:
        print("No sign-in popup found.")

def load_and_scroll_page(driver, url, scroll_pause=3, max_scrolls=100):
    driver.get(url)
    handle_popups(driver)

    scroll_attempt = 0
    last_height = driver.execute_script("return document.body.scrollHeight")

    while scroll_attempt < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)

        try:
            load_more = driver.find_element(By.XPATH, "//button[contains(text(), 'Load more results')]")
            if load_more.is_displayed() and load_more.is_enabled():
                print(f"[Scroll {scroll_attempt + 1}] Clicking 'Load more results'...")
                load_more.click()
                time.sleep(scroll_pause)
        except:
            print(f"[Scroll {scroll_attempt + 1}] No 'Load more results' button or not clickable.")

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print("[i] Reached end of results.")
            break
        last_height = new_height
        scroll_attempt += 1

    page_source = driver.page_source
    driver.quit()
    return page_source

# Data Extraction
def extract_hotels_data(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    hotels = soup.find_all('div', {'data-testid': 'property-card'})

    hotels_data = []
    for hotel in hotels:
        name = hotel.find('div', {'data-testid': 'title'})
        link = hotel.find('a', {'data-testid': 'title-link'})
        location = hotel.find('span', {'data-testid': 'address'})
        price = hotel.find('span', {'data-testid': 'price-and-discounted-price'})
        rating_element = hotel.find('div', {'data-testid': 'review-score'})

        hotels_data.append({
            'name': name.text.strip() if name else 'N/A',
            'link': link['href'] if link else 'N/A',
            'location': location.text.strip() if location else 'N/A',
            'price': price.text.strip() if price else 'N/A',
            'rating': rating_element.text.strip().split()[1] if rating_element else 'N/A'
        })

    return pd.DataFrame(hotels_data)

# Transformation / Cleaning
def clean_price(price):
    return price.replace('\u00a3', '').replace(',', '').strip()

def transform_data(df):
    df['price'] = df['price'].apply(clean_price)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['location'] = 'Lagos'
    return df

# Feature Engineering
def categorize_rating(rating):
    if pd.isna(rating):
        return 'N/A'
    elif rating >= 9.0:
        return 'Excellence'
    elif rating >= 8.0:
        return 'Very Good'
    elif rating >= 7.0:
        return 'Good'
    elif rating >= 6.0:
        return 'Fair'
    else:
        return 'Average'

def add_features(df):
    df['rating_category'] = df['rating'].apply(categorize_rating)
    df['scrapped_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return df

# Data Loading
def load_to_csv(df, filename='hotels_data.csv'):
    df.to_csv(filename, index=False)

def load_to_db(df):
    load_dotenv()
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    with engine.connect() as connection:
        df.to_sql('hotels_data', connection, if_exists='replace', index=False)
        print("Data loaded successfully into the database!")

# ----------------------------------
# Run All
# ----------------------------------
def main():
    url = " " # Paste the url here
    driver = configure_driver()
    page_source = load_and_scroll_page(driver, url)
    raw_data = extract_hotels_data(page_source)
    cleaned_data = transform_data(raw_data)
    featured_data = add_features(cleaned_data)
    load_to_csv(featured_data)
    load_to_db(featured_data)

# if __name__ == '__main__':
    main()
