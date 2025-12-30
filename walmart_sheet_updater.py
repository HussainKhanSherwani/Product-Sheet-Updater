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
import os

# --- CONFIGURATION ---
try:
    LOG_FILE = "scraper.log"

    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {msg}"
        print(line, flush=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # Clear old logs if running new session
    open(LOG_FILE, "w").close()
    log(" Walmart sheet updater started...")

    # --- CONFIGURATION ---
    GCP_CREDENTIALS_FILE = 'credentials.json'
    SCRAPING_ANT_API_KEY = st.secrets["api_keys"]["scraping_ant"]
    TARGET_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1miyn4Y1UZKgJRcOEwKQ6qJCG94tBUFSiGThA3AQI2TU/edit?gid=1224872406#gid=1224872406'

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

    # --- Handle start_row / end_row from command-line args ---
    if len(sys.argv) >= 3:
        start_row = int(sys.argv[1])
        end_row = int(sys.argv[2])

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

    # --- Helper to convert column number to letter (Handles A-Z, AA-ZZ, etc.) ---
    def get_col_letter(col_idx):
        """Converts 1 -> A, 27 -> AA, 28 -> AB"""
        string = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            string = chr(65 + remainder) + string
        return string

    link_col = col("Walmart Link")
    today_price_col = col("Today Price")
    old_price_col = col("Old Price")
    today_stock_col = col("Today Stock")
    old_stock_col = col("Old Stock")
    buybox_col = col("BuyBox Winner")
    date_col = col("Stock Update Date")

    # --- STEP 1: Copy Today → Old (once for all rows) ---
    log("🔁 Copying Today → Old columns...")

    old_price_values = [[row[today_price_col - 1]] for row in rows[:end_row - start_row + 1]]
    old_stock_values = [[row[today_stock_col - 1]] for row in rows[:end_row - start_row + 1]]

    log(old_price_values)
    log(old_stock_values)
    # --- Dynamically calculate update ranges (FIXED) ---
    old_price_range = f"{get_col_letter(old_price_col)}{start_row}:{get_col_letter(old_price_col)}{end_row}"
    old_stock_range = f"{get_col_letter(old_stock_col)}{start_row}:{get_col_letter(old_stock_col)}{end_row}"
    log(f"    ↳ Old Price Range: {old_price_range}")
    log(f"    ↳ Old Stock Range: {old_stock_range}")

    # --- Push updates ---
    sheet.update(old_price_range, old_price_values)
    sheet.update(old_stock_range, old_stock_values)

    log("✅ Old Price and Old Stock columns updated.\n")
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
                log(f"❌ ScrapingAnt failed ({res.status}) for {url}")
                return None
            html = res.read().decode("utf-8")
            return html
        except Exception as e:
            log(f"⚠️ Exception fetching {url}: {e}")
            return None
        finally:
            try:
                conn.close()
            except:
                pass

    # --- Walmart HTML Parser ---
    def parse_walmart_html(html):
        soup = BeautifulSoup(html, "html.parser")

        # Seller
        seller_tag = soup.find("span", attrs={"data-testid": "product-seller-info"})
        seller_name = ""
        if seller_tag:
            link_tag = seller_tag.find("a", attrs={"data-testid": "seller-name-link"})
            if link_tag:
                seller_name = link_tag.get_text(strip=True)
            else:
                seller_name = seller_tag.get_text(strip=True).replace("Sold and shipped by", "").strip()
            seller_name = re.sub(r'\.com.*$', '', seller_name).strip()

        # Stock
        stock_status = 0
        low_stock_span = soup.find("span", class_="w_yTSq f7 f6-hdkp lh-solid lh-title-hdkp b dark-red w_0aYG w_MwbK")
        if low_stock_span:
            span_text = low_stock_span.get_text(strip=True)
            match = re.search(r"(\d+)", span_text)
            if match:
                stock_status = match.group(1)
            else:
                stock_status = "10"
        else:
            unavailable_tag = soup.find("span", class_="b mr1")
            # 1. Primary Check: Look at the unavailable_tag first
            if unavailable_tag:
                unavailable_text = unavailable_tag.get_text().strip()

                # Case A: Explicit "Out of stock" -> Immediate 0
                if "Out of stock" in unavailable_text:
                    stock_status = 0
                    print(f"    ↳ Detected 'Out of stock' directly.")
                # Case B: "Not available" -> Dig deeper into fulfillment tag
                elif "Not available" in unavailable_text:
                    print(f"    ↳ Detected 'Not available', checking fulfillment tag...")
                    # Look for the fulfillment tag (case-insensitive for 'shipping')
                    fulfillment_tag = soup.find('div', attrs={'data-seo-id': re.compile(r'fulfillment-shipping-intent', re.IGNORECASE)})
                    if fulfillment_tag:
                        tag_text = fulfillment_tag.get_text().strip()
                        print(f"      ↳ Fulfillment tag text: {tag_text}")
                        # Sub-check 1: Still says out of stock
                        if "Out of stock" in tag_text:
                            stock_status = 0
                            print(f"      ↳ Detected 'Out of stock' in fulfillment tag.")
                        # Sub-check 2: Says "Arrives [Date]" -> In Stock
                        elif "Arrives" in tag_text:
                            stock_status = 100
                            print(f"      ↳ Detected 'Arrives' in fulfillment tag, marking as in stock.")
            else:
                stock_status = 100 if seller_tag else 0

        # Price
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
            log(f"    ↳ scraping: {link}")
            html = None

            # --- Retry fetching HTML up to 3 times ---
            for attempt in range(3):
                html = fetch_html_with_scrapingant(link)
                if html:
                    break
                log(f"      ⚠️ Fetch attempt {attempt+1} failed, retrying...")
                time.sleep(10)

            if not html:
                log(f"      ❌ failed all 3 fetch attempts for {link}")
                continue

            # --- Parse page (with retry if price missing) ---
            price, stock, seller = parse_walmart_html(html)

            # Retry price parse logic
            if price is None:
                log(f"      ⚠️ Price missing, retrying parse for {link}...")
                retry_price = None
                for attempt in range(2):
                    time.sleep(10)
                    html_retry = fetch_html_with_scrapingant(link)
                    if not html_retry:
                        continue
                    price_retry, stock_retry, seller_retry = parse_walmart_html(html_retry)
                    if price_retry is not None:
                        price, stock, seller = price_retry, stock_retry, seller_retry
                        retry_price = price_retry
                        break
                if retry_price is None:
                    log(f"      ❌ Price still missing after 3 attempts for {link}")
                    price = ""

            if price:
                try:
                    total_price += float(price)
                except:
                    pass
            if seller:
                sellers.add(seller)
            
            if stock == 0:
                stock_values.append(0)
            elif stock == 100:
                stock_values.append(100)
            else:
                try:
                    stock_values.append(int(stock))
                except:
                    stock_values.append(10)

            time.sleep(5) # Delay between links in same cell

        if not links:
            return "", 0, ""

        final_price = round(total_price, 2) if total_price else ""
        final_stock = (
            0
            if not stock_values or 0 in stock_values
            else str(min(stock_values)) if min(stock_values) <= 10 else "100"
        )
        final_seller = ", ".join(sorted(sellers)) if sellers else ""

        return final_price, final_stock, final_seller

    # --- STEP 2: Scraping Loop ---
    log("🕷 Starting scrape (max 300 items)...\n")
    batch_size = 300
    update_chunk = 2

    batch_prices, batch_stocks, batch_buyboxes, batch_dates, batch_rows = ([] for _ in range(5))
    failed_rows_indices = []  # NEW: Track failed rows

    for idx, row in enumerate(rows[:batch_size], start=start_row):
        url_str = row[link_col - 1].strip()
        price = ""
        stock = 0
        seller_name = ""

        if not url_str:
            pass
        else:
            log(f"🔍 Row {idx}: {url_str}")
            price, stock, seller_name = scrape_multiple_walmart_links(url_str)
            print(f"🔍 Row {idx}: price: {price}, stock: {stock}")

            # NEW: If price is missing (failed scrape), add to tracking list
            if price == "" or price is None:
                log(f"⚠️ Row {idx} failed to get price. Added to retry list.")
                failed_rows_indices.append(idx)

            # --- Fallback to Old Values ---
            old_price = row[old_price_col - 1] if len(row) >= old_price_col else ""
            old_stock = row[old_stock_col - 1] if len(row) >= old_stock_col else ""
            old_buybox = row[buybox_col - 1] if len(row) >= buybox_col else ""

            if not price or price == "":
                price = old_price or ""
            if not stock:
                stock = 0
            if not seller_name or seller_name.strip() == "":
                seller_name = old_buybox or ""

            log(f"✅ {idx}: price={price}, stock={stock}, buybox={seller_name}")

        time.sleep(3) # Delay between rows

        # Append to batch
        batch_prices.append([price or ""])
        batch_stocks.append([stock])
        batch_buyboxes.append([seller_name])
        batch_dates.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        batch_rows.append(idx)

        if len(batch_rows) >= update_chunk:
            start_row = batch_rows[0]
            end_row = batch_rows[-1]
            log(f"📤 Writing rows {start_row}-{end_row}...")
            # --- Perform all updates in this batch (FIXED) ---
            sheet.update(f"{get_col_letter(today_price_col)}{start_row}:{get_col_letter(today_price_col)}{end_row}", batch_prices)
            sheet.update(f"{get_col_letter(today_stock_col)}{start_row}:{get_col_letter(today_stock_col)}{end_row}", batch_stocks)
            sheet.update(f"{get_col_letter(buybox_col)}{start_row}:{get_col_letter(buybox_col)}{end_row}", batch_buyboxes)
            sheet.update(f"{get_col_letter(date_col)}{start_row}:{get_col_letter(date_col)}{end_row}", batch_dates)
            log(f"✅ Updated rows {start_row}-{end_row}\n")

            # Clear batch
            batch_prices, batch_stocks, batch_buyboxes, batch_dates, batch_rows = ([] for _ in range(5))

    if len(batch_rows) > 0:
        start_row = batch_rows[0]
        end_row = batch_rows[-1]
        log(f"📤 Writing final rows {start_row}-{end_row}...")
        # --- Perform all updates in this batch (FIXED) ---
        sheet.update(f"{get_col_letter(today_price_col)}{start_row}:{get_col_letter(today_price_col)}{end_row}", batch_prices)
        sheet.update(f"{get_col_letter(today_stock_col)}{start_row}:{get_col_letter(today_stock_col)}{end_row}", batch_stocks)
        sheet.update(f"{get_col_letter(buybox_col)}{start_row}:{get_col_letter(buybox_col)}{end_row}", batch_buyboxes)
        sheet.update(f"{get_col_letter(date_col)}{start_row}:{get_col_letter(date_col)}{end_row}", batch_dates)
        log(f"✅ Updated rows {start_row}-{end_row}\n")

    log(f"🎉 Done! All {end_row - start_row + 1} rows scraped and updated in batches of 5.")

    # --- NEW: RETRY PHASE ---
    if failed_rows_indices:
        log(f"\n🔄 --- RETRY PHASE: Attempting {len(failed_rows_indices)} failed rows again ---")
        log(f"Failed Rows Indices: {failed_rows_indices}")
        # We reuse batch lists for the retry updates
        # batch_prices, batch_stocks, batch_buyboxes, batch_dates, batch_rows = ([] for _ in range(5)) # [REMOVED]
        
        for idx in failed_rows_indices:
            # We must fetch the URL again from the original data (idx is 1-based, data is 0-based)
            # data includes header, so idx 1 is data[0]. Wait, rows started at start_row.
            # Easiest way: just grab from the 'data' variable which holds the WHOLE sheet.
            # data[0] is header. data[1] is Row 2. So data[idx-1] is Row idx.
            
            try:
                row_data = data[idx - 1]
                url_str = row_data[link_col - 1].strip()
                log(f"🔄 Retrying Row {idx}: {url_str}")
                
                # Scrape again
                price, stock, seller_name = scrape_multiple_walmart_links(url_str)
                
                # Check if successful THIS time
                if price and price != "":
                    log(f"✅ Retry SUCCESS for Row {idx}! New Price: {price}")
                    
                    # Update this SINGLE row immediately to ensure it saves
                    # (We could batch, but for retries, immediate safety is often better)
                    sheet.update_cell(idx, today_price_col, price)
                    sheet.update_cell(idx, today_stock_col, stock)
                    sheet.update_cell(idx, buybox_col, seller_name)
                    sheet.update_cell(idx, date_col, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    log(f"❌ Retry FAILED again for Row {idx}. Leaving fallback values.")
            
            except Exception as e:
                log(f"⚠️ Error during retry for row {idx}: {e}")
            
            time.sleep(3) # Courtesy delay in retry loop

    log(f"🎉 Done! All rows processed.")
    if os.path.exists("start.txt"):
        os.remove("start.txt")

except Exception as e:
    log(f" Fatal error: {e}")
    if os.path.exists("start.txt"):
        os.remove("start.txt")