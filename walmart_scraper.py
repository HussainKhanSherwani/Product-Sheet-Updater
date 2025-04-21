import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st
import time

SCRAPING_ANT_API_KEY = st.secrets["api_keys"]["scraping_ant"]

# Define the scope
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# Load Google service account credentials
credentials_dict = st.secrets["gcp_service_account"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
gc = gspread.authorize(credentials)


def get_worksheet_from_url(sheet_url):
    worksheet = gc.open_by_url(sheet_url).sheet1
    return worksheet


def scrape_walmart_product(url, max_retries=3):
    for attempt in range(max_retries):
        api_url = f'https://api.scrapingant.com/v2/general?url={url}&x-api-key={SCRAPING_ANT_API_KEY}&browser=false'
        response = requests.get(api_url)

        if response.status_code == 200:
            break
        elif response.status_code == 409:
            print("409 Conflict. Retrying...")
            time.sleep(2)
        else:
            print(f"❌ Attempt {attempt + 1} failed for URL: {url} - Status code: {response.status_code}")
            if attempt == max_retries - 1:
                return None, False,True  # Failed after retries
    try:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        # Check "Sold and shipped by Walmart.com"
        seller_tag = soup.find("span", attrs={"data-testid": "product-seller-info"})
        in_stock = seller_tag and "Walmart.com" in seller_tag.text
        if not in_stock:
            # Check if out of stock
            unavailable_tag = soup.find("span", class_="b mr1")
            if not unavailable_tag:
                 return None, False,True #error 
            not_available = "Not available" in unavailable_tag.text
            if not_available:
                return None, False,False  # Not in stock    
        # Extract price
        price_tag = soup.find("span", attrs={"itemprop": "price", "data-seo-id": "hero-price"})
        price = None
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return None, False,True

    if price_tag:
        price_text = price_tag.text.strip().replace('$', '').replace(',', '')
        try:
            price = float(price_text)
        except ValueError:
            pass

    return price, in_stock,False


def update_google_sheet(worksheet):
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    for idx, row in df.iterrows():
        url = row['Item Link']
        old_price = row.get('Old Price', None)

        print(f"Scraping: {url}")
        try:
            new_price,in_stock,error = scrape_walmart_product(url)
            print(f"→ Price: {new_price}, In Stock: {in_stock} Error: {error}")
        except Exception as e:
            print(f"❌ Error scraping: {e}")
            continue
        row_index = idx + 2  # account for header row
        
        if error:
            print(f"Error scraping {url}, skipping...")
            worksheet.update_cell(idx + 2, df.columns.get_loc('New Price') + 1,'Check manually(error occured)')
            worksheet.update_cell(row_index, df.columns.get_loc('In Stock') + 1, 'Check manually(error occured)')
            worksheet.update_cell(row_index, df.columns.get_loc('Price change') + 1, "0")
        else:
            # Update "In Stock"
            worksheet.update_cell(row_index, df.columns.get_loc('In Stock') + 1, 'OOS' if not in_stock else 'Yes')

            # Update "New Price" and "Price change"
            if new_price is not None and old_price is not None:
                worksheet.update_cell(row_index, df.columns.get_loc('New Price') + 1, new_price)
                if new_price != old_price:
                    change = round(new_price - old_price, 2)
                    worksheet.update_cell(row_index, df.columns.get_loc('Price change') + 1, f"{'+' if change > 0 else ''}{change}")
                else:
                    worksheet.update_cell(row_index, df.columns.get_loc('Price change') + 1, "0")
            else:
                worksheet.update_cell(row_index, df.columns.get_loc('New Price') + 1, new_price or '')
                worksheet.update_cell(row_index, df.columns.get_loc('Price change') + 1, '')



