import streamlit as st
import subprocess
import time
import os

st.set_page_config(page_title="Walmart Sheet Updater", layout="wide")

st.title("🛒 Walmart Sheet Updater Dashboard")

# --- Input controls ---
start_row = st.number_input("Start Row", min_value=2, value=3, step=1)
end_row = st.number_input("End Row", min_value=2, value=3, step=1)

if start_row > end_row:
    st.error("❌ Start row must be less than or equal to End row.")
    st.stop()

log_file = "scraper.log"

# --- Start button ---
if st.button("🚀 Start Scraper"):
    # Clear old log file
    open(log_file, "w").close()

    cmd = ["python", "walmart_sheet_updater.py", str(start_row), str(end_row)]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    st.session_state["process"] = process
    st.session_state["running"] = True
    st.success(f"✅ Scraper started for rows {start_row}-{end_row}...")

# --- Live Log Display ---
st.subheader("📜 Live Logs")

log_display = st.empty()

if os.path.exists(log_file):
    with open(log_file, "r", encoding="utf-8") as f:
        logs = f.read()
    log_display.text_area("Logs", logs, height=400)

# --- Auto-refresh logs ---
if "running" in st.session_state and st.session_state["running"]:
    while True:
        time.sleep(2)
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                logs = f.read()
            log_display.text_area("Logs", logs, height=400)
        else:
            log_display.text_area("Logs", "Waiting for logs...", height=400)

        # Check if scraper process finished
        if st.session_state["process"].poll() is not None:
            st.success("🎉 Scraper completed successfully.")
            st.session_state["running"] = False
            break
