import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st
import time

# Setup
SCRAPING_ANT_API_KEY = st.secrets["api_keys"]["scraping_ant"]

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
credentials_dict = st.secrets["gcp_service_account"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
gc = gspread.authorize(credentials)


# Helper to get worksheet
def get_worksheet_from_url(sheet_url):
    worksheet = gc.open_by_url(sheet_url).sheet1
    return worksheet


# General retry function
def fetch_html_with_retries(url, max_retries=3):
    for attempt in range(max_retries):
        api_url = f'https://api.scrapingant.com/v2/general?url={url}&x-api-key={SCRAPING_ANT_API_KEY}&browser=true'
        response = requests.get(api_url)

        if response.status_code == 200:
            return response.text
        elif response.status_code == 409:
            print("409 Conflict. Retrying...")
            time.sleep(2)
        else:
            print(f"❌ Attempt {attempt + 1} failed for URL: {url} - Status code: {response.status_code}")
            if attempt == max_retries - 1:
                return None
    return None


# Walmart scraper
def scrape_walmart_product(url, max_retries=3):
    html = fetch_html_with_retries(url, max_retries)
    if not html:
        return None, False, True

    try:
        soup = BeautifulSoup(html, 'html.parser')

        seller_tag = soup.find("span", attrs={"data-testid": "product-seller-info"})
        in_stock = seller_tag and "Walmart.com" in seller_tag.text

        if not in_stock:
            unavailable_tag = soup.find("span", class_="b mr1")
            if not unavailable_tag:
                return None, False, True
            not_available = "Not available" in unavailable_tag.text
            if not_available:
                return None, False, False

        price_tag = soup.find("span", attrs={"itemprop": "price", "data-seo-id": "hero-price"})
        price = None

        if price_tag:
            price_text = price_tag.text.strip().replace('$', '').replace(',', '')
            try:
                price = float(price_text)
            except ValueError:
                pass

        return price, in_stock, False

    except Exception as e:
        print(f"Error parsing Walmart HTML: {e}")
        return None, False, True


# Amazon scraper
# Amazon scraper
def scrape_amazon_product(url, max_retries=3):
    cannot_ship_text = "This item cannot be shipped to your selected delivery location. Please choose a different delivery location."
    
    for attempt in range(max_retries):
        html = fetch_html_with_retries(url, max_retries)
        if not html:
            return None, False, True

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Check for delivery restriction message
            availability_tag = soup.find("div", id="availability")
            if availability_tag:
                availability_text = availability_tag.get_text(strip=True)
                if cannot_ship_text in availability_text:
                    if attempt < max_retries - 1:
                        print(f"⚠️ Attempt {attempt + 1}: Cannot ship to location, retrying...")
                        time.sleep(2)
                        continue
                    else:
                        return cannot_ship_text, False, False  # Final fallback after retries

            # Price extraction
            price = None
            price_span = soup.find('span', class_='a-price aok-align-center reinventPricePriceToPayMargin priceToPay')
            if price_span:
                price_whole = price_span.find('span', class_='a-price-whole')
                price_fraction = price_span.find('span', class_='a-price-fraction')
                if price_whole and price_fraction:
                    price_text = price_whole.get_text(strip=True).replace(',', '') + price_fraction.get_text(strip=True)
                    try:
                        price = float(price_text)
                    except ValueError:
                        price = None

            # In-stock logic
            in_stock = False
            if availability_tag:
                if ("In Stock" in availability_text) or ("Only" in availability_text and "left in stock" in availability_text):
                    in_stock = True

            return price, in_stock, False

        except Exception as e:
            print(f"Error parsing Amazon HTML: {e}")
            return None, False, True


# eBay scraper
def scrape_ebay_product(url, max_retries=3):
    html = fetch_html_with_retries(url, max_retries)
    if not html:
        return None, False, True, None, None

    try:
        soup = BeautifulSoup(html, 'html.parser')
        # Price Extraction
        price_tag = soup.find("div", {"class": "x-price-primary", "data-testid": "x-price-primary"})
        price = None

        if price_tag:
            price_text_tag = price_tag.find("span", class_="ux-textspans")
            if price_text_tag:
                price_text = price_text_tag.text.strip()
                price_text = price_text.replace('US', '').replace('$', '').replace('/ea', '').replace(',', '').strip()
                try:
                    price = float(price_text)
                except ValueError:
                    pass

        # Quantity Info Extraction
        qty_div = soup.find("div", {"id": "qtyAvailability"})
        available_qty = None
        sold_qty = None

        if qty_div:
            spans = qty_div.find_all("span", class_="ux-textspans")
            for span in spans:
                text = span.text.strip()
                if 'available' in text.lower():
                    available_qty = text
                elif 'sold' in text.lower():
                    sold_qty = text

        # Determine in-stock
        in_stock = True if available_qty and ("available" in available_qty.lower()) else False

        return price, in_stock, False, available_qty, sold_qty

    except Exception as e:
        print(f"Error parsing eBay HTML: {e}")
        return None, False, True, None, None

# Decide which scraper to use
def scrape_product(url, max_retries=3):
    if "walmart.com" in url:
        price, in_stock, error = scrape_walmart_product(url, max_retries)
        return price, in_stock, error, None, None
    elif "amazon." in url:
        price, in_stock, error = scrape_amazon_product(url, max_retries)
        return price, in_stock, error, None, None
    elif "ebay." in url:
        return scrape_ebay_product(url, max_retries)
    else:
        print(f"Unsupported URL: {url}")
        return None, False, True, None, None


# Update the Google Sheet
def update_google_sheet(worksheet):
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    for idx, row in df.iterrows():
        url = row['Item Link']
        old_price = row.get('Old Price', None)

        print(f"Scraping: {url}")
        try:
            new_price, in_stock, error, available_qty, sold_qty = scrape_product(url)
            print(f"→ Price: {new_price}, In Stock: {in_stock}, Available: {available_qty}, Sold: {sold_qty}, Error: {error}")
        except Exception as e:
            print(f"❌ Error scraping: {e}")
            continue

        row_index = idx + 2  # account for header row

        if error:
            print(f"Error scraping {url}, skipping...")
            worksheet.update_cell(row_index, df.columns.get_loc('New Price') + 1, 'Check manually(error occurred)')
            worksheet.update_cell(row_index, df.columns.get_loc('In Stock') + 1, 'Check manually(error occurred)')
            worksheet.update_cell(row_index, df.columns.get_loc('Price change') + 1, "0")

            # Optional: Clear available/sold if error (only if columns exist)
            if 'Available Quantity' in df.columns:
                worksheet.update_cell(row_index, df.columns.get_loc('Available Quantity') + 1, '')
            if 'Sold Quantity' in df.columns:
                worksheet.update_cell(row_index, df.columns.get_loc('Sold Quantity') + 1, '')
        else:
            # Update In Stock
            worksheet.update_cell(row_index, df.columns.get_loc('In Stock') + 1, 'OOS' if not in_stock else 'Yes')

            # Update New Price and Price Change
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

            # Update Available Quantity and Sold Quantity if present
            if 'Available Quantity' in df.columns:
                worksheet.update_cell(row_index, df.columns.get_loc('Available Quantity') + 1, available_qty or '')
            if 'Sold Quantity' in df.columns:
                worksheet.update_cell(row_index, df.columns.get_loc('Sold Quantity') + 1, sold_qty or '')
