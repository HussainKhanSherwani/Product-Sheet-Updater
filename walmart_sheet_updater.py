import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
import urllib.parse
import re
import time
from datetime import datetime
import sys
import streamlit as st
import os
from concurrent.futures import ThreadPoolExecutor  # Added for concurrency

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
    SCRAPER_DO_API_KEY = st.secrets["api_keys"]["scraper_do"]
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

    def safe_batch_update(worksheet, data):
        """
        Writes MULTIPLE ranges in ONE API call.
        data format: [{'range': 'A1', 'values': [['v']]}, ...]
        """
        for attempt in range(5):
            try:
                # gspread batch_update takes a list of range objects
                worksheet.batch_update(data)
                return
            except Exception as e:
                if "429" in str(e) or "quota" in str(e).lower():
                    wait_time = 15 + (attempt * 10)
                    log(f"⏳ API Quota hit (Batch). Sleeping {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise e

    # --- ARGUMENT PARSING (Range vs List) ---
    target_rows = []
    is_list_mode = False

    # Default fallbacks
    start_row = 3
    end_row = 3

    if len(sys.argv) >= 3:
        if sys.argv[1] == "list":
            # LIST MODE: Expects comma-separated string "3,5,10"
            is_list_mode = True
            raw_indices = sys.argv[2].split(',')
            # Convert to distinct integers and sort
            target_rows = sorted(list(set([int(x) for x in raw_indices if x.strip().isdigit()])))
            log(f"📋 Mode: Specific Rows ({len(target_rows)} rows: {target_rows})")
        else:
            # RANGE MODE: Expects start end
            start_row = int(sys.argv[1])
            end_row = int(sys.argv[2])
            if start_row > end_row:
                raise ValueError("start_row should be less than or equal to end_row")
            target_rows = list(range(start_row, end_row + 1))
            log(f"📉 Mode: Range ({start_row} to {end_row})")
    else:
        # Fallback default
        target_rows = [3]

    # --- OPEN SHEET ---
    sheet = client.open_by_url(TARGET_SHEET_URL).get_worksheet(0)
    data = sheet.get_all_values()
    header = data[0]
    
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
    flag_col = col("Flag") 

    # --- STEP 1: Copy Today → Old (once for all rows) ---
    log("🔁 Copying Today → Old columns...")

    if is_list_mode:
        # LIST MODE: Update one by one (safest for scattered rows)
        for r_idx in target_rows:
            if r_idx - 1 < len(data):
                row_data = data[r_idx - 1]
                t_price = row_data[today_price_col - 1]
                t_stock = row_data[today_stock_col - 1]
                
                # OPTIMIZATION: Combine these 2 calls into 1 batch for this row
                updates = [
                    {'range': f"{get_col_letter(old_price_col)}{r_idx}", 'values': [[t_price]]},
                    {'range': f"{get_col_letter(old_stock_col)}{r_idx}", 'values': [[t_stock]]}
                ]
                safe_batch_update(sheet, updates)
    else:
        # RANGE MODE: Bulk update (Faster, original logic)
        rows_slice = data[start_row - 1:end_row]
        old_price_values = [[row[today_price_col - 1]] for row in rows_slice]
        old_stock_values = [[row[today_stock_col - 1]] for row in rows_slice]

        log(old_price_values)
        log(old_stock_values)
        
        old_price_range = f"{get_col_letter(old_price_col)}{start_row}:{get_col_letter(old_price_col)}{end_row}"
        old_stock_range = f"{get_col_letter(old_stock_col)}{start_row}:{get_col_letter(old_stock_col)}{end_row}"
        log(f"    ↳ Old Price Range: {old_price_range}")
        log(f"    ↳ Old Stock Range: {old_stock_range}")

        # --- Push updates using SAFE batch ---
        # Note: batch_update takes a LIST of range objects
        updates = [
            {'range': old_price_range, 'values': old_price_values},
            {'range': old_stock_range, 'values': old_stock_values}
        ]
        safe_batch_update(sheet, updates)

    log("✅ Old Price and Old Stock columns updated.\n")
    time.sleep(2)

    # --- ScrapingAnt HTML fetcher (Now using Scraper.do logic) ---
    def fetch_html_with_scrapingant(url):
        targetUrl = urllib.parse.quote(url)
        # Using the exact way you provided
        scrape_do_url = "http://api.scrape.do/?url={}&token={}".format(targetUrl, SCRAPER_DO_API_KEY)
        
        try:
            response = requests.request("get", scrape_do_url, timeout=100)
            if response.status_code != 200:
                log(f"❌ Scraper.do failed ({response.status_code}) for {url}")
                return None
            return response.text
        except Exception as e:
            log(f"⚠️ Exception fetching {url}: {e}")
            return None

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

    # --- Wrapper for concurrency ---
    def process_row(idx):
        if idx - 1 >= len(data):
            return idx, None, None, None, "OUT_OF_BOUNDS"
        
        row = data[idx - 1]
        url_str = row[link_col - 1].strip()
        
        if not url_str:
            return idx, "", 0, "", "SUCCESSFUL"
            
        log(f"🔍 Row {idx}: {url_str}")
        price, stock, seller_name = scrape_multiple_walmart_links(url_str)
        print(f"🔍 Row {idx}: price: {price}, stock: {stock}")
        
        flag_status = "SUCCESSFUL"
        if price == "" or price is None:
            flag_status = "FAILED: Scraper Auto-Retry Again"
            
        # Fallbacks to old data if needed
        old_price = row[old_price_col - 1] if len(row) >= old_price_col else ""
        old_buybox = row[buybox_col - 1] if len(row) >= buybox_col else ""
        
        if not price or price == "":
            price = old_price or ""
        if not stock:
            stock = 0
        if not seller_name or seller_name.strip() == "":
            seller_name = old_buybox or ""
            
        return idx, price, stock, seller_name, flag_status

    # --- STEP 2: Scraping Loop ---
    log(f"🕷 Starting scrape for {len(target_rows)} rows in blocks of 100...\n")
    batch_size = 3000 
    failed_rows_indices = []
    block_size = 100

    # Process in blocks of 100
    for b in range(0, len(target_rows), block_size):
        block = target_rows[b:b + block_size]
        log(f"📦 Processing Block: {block[0]} to {block[-1]}")
        
        # Use ThreadPoolExecutor for 5 concurrent requests within this block
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(process_row, block))

        # Collect all updates for this block of 100
        block_updates = []
        for idx, price, stock, seller_name, flag_status in results:
            if flag_status == "OUT_OF_BOUNDS":
                log(f"⚠️ Row {idx} out of bounds, skipping.")
                continue
                
            if flag_status == "FAILED: Scraper Auto-Retry Again":
                log(f"⚠️ Row {idx} failed to get price. Added to retry list.")
                failed_rows_indices.append(idx)

            log(f"✅ {idx}: price={price}, stock={stock}, buybox={seller_name}, flag={flag_status}")

            # Handle building the updates list for the block
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            block_updates.extend([
                {'range': f"{get_col_letter(today_price_col)}{idx}", 'values': [[price]]},
                {'range': f"{get_col_letter(today_stock_col)}{idx}", 'values': [[stock]]},
                {'range': f"{get_col_letter(buybox_col)}{idx}", 'values': [[seller_name]]},
                {'range': f"{get_col_letter(date_col)}{idx}", 'values': [[ts]]},
                {'range': f"{get_col_letter(flag_col)}{idx}", 'values': [[flag_status]]}
            ])

        # Batch Write the entire block of 100 to the sheet
        if block_updates:
            log(f"📤 Writing Block ({len(block)} rows) to Google Sheets...")
            safe_batch_update(sheet, block_updates)
            log(f"✅ Block complete.\n")

    log(f"🎉 Done! All rows scraped.")

    # --- NEW: RETRY PHASE ---
    final_failed_indices = [] # Track rows that failed AFTER retry

    if failed_rows_indices:
        log(f"\n🔄 --- RETRY PHASE: Attempting {len(failed_rows_indices)} failed rows again ---")
        
        # Retry with concurrency as well
        with ThreadPoolExecutor(max_workers=5) as executor:
            retry_results = list(executor.map(process_row, failed_rows_indices))
            
        for idx, price, stock, seller_name, flag_status in retry_results:
            try:
                if price and price != "":
                    log(f"✅ Retry SUCCESS for Row {idx}! New Price: {price}")
                    
                    # SINGLE BATCH Update for Retry
                    retry_updates = [
                        {'range': f"{get_col_letter(today_price_col)}{idx}", 'values': [[price]]},
                        {'range': f"{get_col_letter(today_stock_col)}{idx}", 'values': [[stock]]},
                        {'range': f"{get_col_letter(buybox_col)}{idx}", 'values': [[seller_name]]},
                        {'range': f"{get_col_letter(date_col)}{idx}", 'values': [[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]},
                        {'range': f"{get_col_letter(flag_col)}{idx}", 'values': [["SUCCESSFUL"]]}
                    ]
                    safe_batch_update(sheet, retry_updates)
                    
                else:
                    log(f"❌ Retry FAILED again for Row {idx}. Leaving fallback values.")
                    final_failed_indices.append(idx)
                    safe_batch_update(sheet, [{'range': f"{get_col_letter(flag_col)}{idx}", 'values': [["FAILED: Manual Entry Required"]]}])
            except Exception as e:
                log(f"⚠️ Error during retry for row {idx}: {e}")
                final_failed_indices.append(idx)

    # --- FINAL REPORT ---
    if final_failed_indices:
        failed_str = ",".join(map(str, sorted(final_failed_indices)))
        log(f"\n⚠️ FINAL FAILED ROWS: {failed_str}")
        print(f"⚠️ FINAL FAILED ROWS: {failed_str}")

    log(f"🎉 Done! All rows processed.")
    if os.path.exists("start.txt"):
        os.remove("start.txt")

except Exception as e:
    log(f" Fatal error: {e}")
    if os.path.exists("start.txt"):
        os.remove("start.txt")