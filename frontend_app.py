import streamlit as st
import subprocess
import sys
from datetime import datetime

st.set_page_config(page_title="Walmart Sheet Updater", page_icon="🛒", layout="centered")

st.title("🕷 Walmart Sheet Updater")
st.caption("Automatically scrape Walmart links from Google Sheet and update price, stock, and seller info.")

# --- Input controls ---
start_row = st.number_input("Start Row (excluding header)", min_value=2, value=2, step=1)
end_row = st.number_input("End Row", min_value=start_row, value=start_row + 5, step=1)

st.divider()

# --- Run button ---
if st.button("🚀 Run Walmart Scraper"):
    st.info(f"Running scraper from row {start_row} to {end_row}...")

    # --- Create placeholder for live logs ---
    log_area = st.empty()
    logs = ""

    # --- Timestamp helper ---
    def ts(msg):
        return f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"

    try:
        # --- Run the scraper as subprocess ---
        process = subprocess.Popen(
            [sys.executable, "walmart_sheet_updater.py", str(start_row), str(end_row)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        # --- Stream live logs ---
        for line in process.stdout:
            line = line.strip()
            if not line:
                continue
            logs += ts(line) + "\n"
            log_area.text_area("📜 Live Logs", logs, height=400)

        process.wait()  # wait for completion

        # --- Final status ---
        if process.returncode == 0:
            st.success("✅ Walmart Sheet successfully updated!")
        else:
            st.error("❌ Script exited with errors — check logs above.")

    except Exception as e:
        st.error(f"⚠️ Failed to run scraper: {e}")

st.divider()
st.caption("💡 Tip: You can monitor live scraping logs above in real time.")
