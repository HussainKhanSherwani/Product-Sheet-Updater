import streamlit as st
import subprocess
import time
import os
from streamlit_autorefresh import st_autorefresh

LOG_FILE = "scraper_logs.txt"

st.set_page_config(page_title="🛒 Walmart Sheet Updater", layout="centered")

st.title("🧾 Walmart Sheet Updater")
st.caption("Run Walmart scraper for selected rows and monitor progress in real-time.")

# --- Input controls ---
st.sidebar.header("Settings")

start_row = st.sidebar.number_input("Start Row (≥2)", min_value=2, value=2, step=1)
end_row = st.sidebar.number_input("End Row (≥2)", min_value=2, value=3, step=1)

# Ensure valid range
if start_row > end_row:
    st.sidebar.error("⚠️ Start row must be less than or equal to End row.")
    st.stop()

# --- Buttons ---
col1, col2 = st.columns([1, 1])
run_clicked = col1.button("▶️ Run Scraper")
stop_clicked = col2.button("⏹ Stop Scraper")

# --- State initialization ---
if "process" not in st.session_state:
    st.session_state.process = None
if "running" not in st.session_state:
    st.session_state.running = False

# --- Run Scraper ---
if run_clicked:
    # Clear old logs
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    cmd = [
        "python3",
        "walmart_sheet_updater.py",
        str(start_row),
        str(end_row)
    ]

    st.session_state.process = subprocess.Popen(
        cmd, stdout=open(LOG_FILE, "a"), stderr=subprocess.STDOUT, text=True
    )
    st.session_state.running = True
    st.success(f"🚀 Scraper started for rows {start_row} to {end_row}.")

# --- Stop Scraper ---
if stop_clicked and st.session_state.running:
    st.session_state.process.terminate()
    st.session_state.running = False
    st.warning("🛑 Scraper stopped by user.")

# --- Log Viewer ---
st.subheader("📜 Live Logs")

if os.path.exists(LOG_FILE):
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        logs = f.read()
    st.text_area("Logs", logs, height=400, key="log_viewer")
else:
    st.info("No logs yet. Run the scraper to start logging.")

# --- Auto-refresh logs every 3s if running ---
if st.session_state.running:
    st_autorefresh(interval=3000, key="log_refresh")
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            logs = f.read()
        st.text_area("Logs", logs, height=400, key="log_viewer_refresh")

    process = st.session_state.process
    if process.poll() is not None:  # finished
        st.session_state.running = False
        if process.returncode == 0:
            st.success("✅ Walmart Sheet successfully updated!")
        else:
            st.error("❌ Scraper exited with an error. Check logs for details.")

st.markdown("---")
st.caption("💡 Tip: Refreshing the page will not interrupt the running process — logs will continue to stream.")
