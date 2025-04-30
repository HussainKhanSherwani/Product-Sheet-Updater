import streamlit as st
from scraper import update_google_sheet,get_worksheet_from_url

st.set_page_config(page_title="🛒 Product Sheet Updater", layout="centered")

st.title("🧾 Product Price Checker")

sheet_url = st.text_input("Paste your **Google Sheet URL** and give edit access to (scraping-bot@productsheetsync.iam.gserviceaccount.com) and then click on update sheet:")


if st.button("🔁 Update Sheet"):
    if sheet_url.strip() == "":
        st.warning("Please enter a Google Sheet URL.")
    else:
        st.info("⏳ Scraping and updating, please wait...")
        try:
            worksheet = get_worksheet_from_url(sheet_url)
            update_google_sheet(worksheet)
            st.success("✅ Sheet updated successfully!")
        except Exception as e:
            st.error(f"❌ Failed to update sheet: {e}")
