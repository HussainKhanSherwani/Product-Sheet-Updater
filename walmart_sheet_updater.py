import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import http.client
import urllib.parse
import re
import time
from datetime import datetime
import sys
import streamlit as st



# --- CONFIGURATION ---
GCP_CREDENTIALS_FILE = 'credentials.json'
SCRAPING_ANT_API_KEY = st.secrets["api_keys"]["scraping_ant"]
TARGET_SHEET_URL = 'https://docs.google.com/spreadsheets/d/10RghapkcMXrvH_LzFfe36d9rSBrxY-6oK5IfUMWG4Fo/edit?gid=1224872406#gid=1224872406'

# --- AUTHORIZATION ---
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
credentials_dict = st.secrets["gcp_service_account"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
gc = gspread.authorize(credentials)
client = gc


start_row = 3
end_row = 3  # fallback default if run manually

# --- Handle start_row / end_row from command-line args (for Streamlit integration) ---
if len(sys.argv) >= 3:
    start_row = int(sys.argv[1])
    end_row = int(sys.argv[2])


#add check that start_row should be less than or equal to end_row
if start_row > end_row:
    raise ValueError("start_row should be less than or equal to end_row")

# --- OPEN SHEET ---
sheet = client.open_by_url(TARGET_SHEET_URL).get_worksheet(0)
data = sheet.get_all_values()
header = data[0]
rows = data[start_row - 1:end_row]  # skip header

# --- Column helper ---
def col(name):
    return header.index(name) + 1

link_col = col("Walmart Link")
today_price_col = col("Today Price")
old_price_col = col("Old Price")
today_stock_col = col("Today Stock")
old_stock_col = col("Old Stock")
buybox_col = col("BuyBox Winner")
date_col = col("Stock Update Date")
diff_col = col("Price Difference")

# --- STEP 1: Copy Today → Old (once for all rows) ---
print("🔁 Copying Today → Old columns...")

# --- Prepare old data to copy ---
old_price_values = [[row[today_price_col - 1]] for row in rows[:end_row - start_row + 1]]
old_stock_values = [[row[today_stock_col - 1]] for row in rows[:end_row - start_row + 1]]

print(old_price_values)
print(old_stock_values)
# --- Dynamically calculate update ranges ---
old_price_range = f"{chr(64 + old_price_col)}{start_row}:{chr(64 + old_price_col)}{end_row}"
old_stock_range = f"{chr(64 + old_stock_col)}{start_row}:{chr(64 + old_stock_col)}{end_row}"
print(f"    ↳ Old Price Range: {old_price_range}")
print(f"    ↳ Old Stock Range: {old_stock_range}")

# --- Push updates ---
sheet.update(old_price_range, old_price_values)
sheet.update(old_stock_range, old_stock_values)

print("✅ Old Price and Old Stock columns updated.\n")
time.sleep(2)

# --- ScrapingAnt HTML fetcher ---
def fetch_html_with_scrapingant(url):
    encoded_url = urllib.parse.quote(url, safe='')
    path = f"/v2/general?url={encoded_url}&x-api-key={SCRAPING_ANT_API_KEY}&browser=true"

    try:
        conn = http.client.HTTPSConnection("api.scrapingant.com")
        conn.request("GET", path)
        res = conn.getresponse()
        if res.status != 200:
            print(f"❌ ScrapingAnt failed ({res.status}) for {url}")
            return None
        html = res.read().decode("utf-8")
        return html
    except Exception as e:
        print(f"⚠️ Exception fetching {url}: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass

# --- Walmart HTML Parser ---
def parse_walmart_html(html):
    soup = BeautifulSoup(html, "html.parser")

    # --- Seller info (Buy Box) ---
    seller_tag = soup.find("span", attrs={"data-testid": "product-seller-info"})
    seller_name = ""

    if seller_tag:
        link_tag = seller_tag.find("a", attrs={"data-testid": "seller-name-link"})
        if link_tag:
            seller_name = link_tag.get_text(strip=True)
        else:
            seller_name = seller_tag.get_text(strip=True).replace("Sold and shipped by", "").strip()
        seller_name = re.sub(r'\.com.*$', '', seller_name).strip()

    # --- Stock status ---
    stock_status = "oos"  # default

    # Check for low stock span directly
    low_stock_span = soup.find(
        "span",
        class_="w_yTSq f7 f6-hdkp lh-solid lh-title-hdkp b dark-red w_0aYG w_MwbK"
    )
    if low_stock_span:
        span_text = low_stock_span.get_text(strip=True)
        match = re.search(r"(\d+)", span_text)
        if match:
            stock_status = match.group(1)
        else:
            stock_status = "10"
    else:
        unavailable_tag = soup.find("span", class_="b mr1")
        if unavailable_tag and ("Out of stock" in unavailable_tag.text or "Not available" in unavailable_tag.text):
            stock_status = "oos"
        else:
            stock_status = "10+" if seller_tag else "oos"

    # --- Price ---
    price = None
    price_tag = soup.find("span", attrs={"itemprop": "price", "data-seo-id": "hero-price"})
    if price_tag:
        price_text = price_tag.text.strip()
        m = re.search(r"\$?([\d.,]+)", price_text)
        if m:
            price = round(float(m.group(1).replace(',', '')), 2)

    return price, stock_status, seller_name


def scrape_multiple_walmart_links(links_str):
    """Scrape one or more Walmart links, aggregate price/stock/seller."""
    links = re.split(r'[,\s|]+', links_str.strip())
    links = [l for l in links if l.startswith("http")]

    total_price = 0.0
    stock_values = []
    sellers = set()

    for link in links:
        print(f"    ↳ scraping: {link}")
        html = None

        # --- Retry fetching HTML up to 3 times ---
        for attempt in range(3):
            html = fetch_html_with_scrapingant(link)
            if html:
                break
            print(f"      ⚠️ Fetch attempt {attempt+1} failed, retrying...")
            time.sleep(2)

        if not html:
            print(f"      ❌ failed all 3 fetch attempts for {link}")
            continue

        # --- Parse page (with retry if price missing) ---
        price, stock, seller = parse_walmart_html(html)

        if price is None:
            print(f"      ⚠️ Price missing, retrying parse for {link}...")
            retry_price = None
            for attempt in range(2):  # two more retries
                time.sleep(2)
                html_retry = fetch_html_with_scrapingant(link)
                if not html_retry:
                    continue
                price_retry, stock_retry, seller_retry = parse_walmart_html(html_retry)
                if price_retry is not None:
                    price, stock, seller = price_retry, stock_retry, seller_retry
                    retry_price = price_retry
                    break
            if retry_price is None:
                print(f"      ❌ Price still missing after 3 attempts for {link}")
                price = ""

        # --- Aggregate results ---
        if price:
            try:
                total_price += float(price)
            except:
                pass

        if seller:
            sellers.add(seller)

        if stock == "oos":
            stock_values.append(0)
        elif stock == "10+":
            stock_values.append(11)
        else:
            try:
                stock_values.append(int(stock))
            except:
                stock_values.append(10)

        time.sleep(1.2)

    # --- Aggregate logic across all links ---
    if not links:
        return "", "oos", ""

    final_price = round(total_price, 2) if total_price else ""
    final_stock = (
        "oos"
        if not stock_values or 0 in stock_values
        else str(min(stock_values)) if min(stock_values) <= 10 else "10+"
    )
    final_seller = ", ".join(sorted(sellers)) if sellers else ""

    return final_price, final_stock, final_seller


# --- STEP 2: Scraping Loop (limit 300, batch write every 5) ---
print("🕷 Starting scrape (max 300 items)...\n")
batch_size = 300
update_chunk = 2

batch_prices, batch_stocks, batch_buyboxes, batch_dates, batch_diffs, batch_rows = ([] for _ in range(6))

for idx, row in enumerate(rows[:batch_size], start=start_row):
    url_str = row[link_col - 1].strip()
    price = ""
    stock = "oos"
    seller_name = ""
    diff_val = ""

    if not url_str:
        pass
    else:
        print(f"🔍 Row {idx}: {url_str}")

        price, stock, seller_name = scrape_multiple_walmart_links(url_str)


        # --- Get previous (old) values from sheet ---
        old_price = row[old_price_col - 1] if len(row) >= old_price_col else ""
        old_stock = row[old_stock_col - 1] if len(row) >= old_stock_col else ""
        old_buybox = row[buybox_col - 1] if len(row) >= buybox_col else ""

        # --- Use old values if scraping failed ---
        if not price or price == "":
            price = old_price or ""
        if not stock or stock == "oos":
            stock = old_stock or "oos"
        if not seller_name or seller_name.strip() == "":
            seller_name = old_buybox or ""

        # --- Calculate difference if we have both prices ---
        try:
            old_val = float(old_price) if old_price else 0
            new_val = float(price) if price else 0
            diff_val = round(new_val - old_val, 2)
        except ValueError:
            diff_val = ""

        print(f"✅ {idx}: price={price}, stock={stock}, buybox={seller_name}, diff={diff_val}")



    time.sleep(1.5)

    # Append to batch
    batch_prices.append([price or ""])
    batch_stocks.append([stock])
    batch_buyboxes.append([seller_name])
    batch_dates.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    batch_diffs.append([diff_val])
    batch_rows.append(idx)

    if len(batch_rows) >= update_chunk:
        start_row = batch_rows[0]
        end_row = batch_rows[-1]
        print(f"📤 Writing rows {start_row}-{end_row}...")
        # --- Perform all updates in this batch ---
        sheet.update(f"{chr(64 + today_price_col)}{start_row}:{chr(64 + today_price_col)}{end_row}", batch_prices)
        sheet.update(f"{chr(64 + today_stock_col)}{start_row}:{chr(64 + today_stock_col)}{end_row}", batch_stocks)
        sheet.update(f"{chr(64 + buybox_col)}{start_row}:{chr(64 + buybox_col)}{end_row}", batch_buyboxes)
        sheet.update(f"{chr(64 + date_col)}{start_row}:{chr(64 + date_col)}{end_row}", batch_dates)
        sheet.update(f"{chr(64 + diff_col)}{start_row}:{chr(64 + diff_col)}{end_row}", batch_diffs)
        print(f"✅ Updated rows {start_row}-{end_row}\n")

        # Clear batch
        batch_prices, batch_stocks, batch_buyboxes, batch_dates, batch_diffs, batch_rows = ([] for _ in range(6))

if len(batch_rows) > 0:
    start_row = batch_rows[0]
    end_row = batch_rows[-1]
    print(f"📤 Writing final rows {start_row}-{end_row}...")
    # --- Perform all updates in this batch ---
    sheet.update(f"{chr(64 + today_price_col)}{start_row}:{chr(64 + today_price_col)}{end_row}", batch_prices)
    sheet.update(f"{chr(64 + today_stock_col)}{start_row}:{chr(64 + today_stock_col)}{end_row}", batch_stocks)
    sheet.update(f"{chr(64 + buybox_col)}{start_row}:{chr(64 + buybox_col)}{end_row}", batch_buyboxes)
    sheet.update(f"{chr(64 + date_col)}{start_row}:{chr(64 + date_col)}{end_row}", batch_dates)
    sheet.update(f"{chr(64 + diff_col)}{start_row}:{chr(64 + diff_col)}{end_row}", batch_diffs)
    print(f"✅ Updated rows {start_row}-{end_row}\n")

print("🎉 Done! All 300 rows scraped and updated in batches of 5.")
