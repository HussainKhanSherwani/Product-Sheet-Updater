import streamlit as st
import subprocess
import os
import time
import sys

LOG_FILE = "scraper.log"

st.set_page_config(page_title="Walmart Sheet Updater", layout="wide")
st.title("🧾 Walmart Product Sheet Updater")

# --- Auto-refresh every few seconds to show live logs ---
refresh_rate = 5  # seconds
st.markdown(
    f"""
    <meta http-equiv="refresh" content="{refresh_rate}">
    """,
    unsafe_allow_html=True,
)

# --- Initialize session state ---
if "running" not in st.session_state:
    st.session_state.running = False
if "logs" not in st.session_state:
    st.session_state.logs = ""
if "process" not in st.session_state:
    st.session_state.process = None

# --- Helper to read logs safely ---
def read_logs():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return "No logs yet."

# --- Sidebar Inputs ---
st.sidebar.header("⚙️ Controls")

start_row = st.sidebar.number_input("Start Row", min_value=2, value=2, step=1)
end_row = st.sidebar.number_input("End Row", min_value=start_row, value=start_row, step=1)

# --- Start / Stop buttons ---
if not st.session_state.running:
    if st.sidebar.button("▶️ Start Update"):
        # Clear log file
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"🚀 Starting Walmart Sheet Updater for rows {start_row}-{end_row}...\n")

        # Run backend with start_row and end_row as arguments
        

        process = subprocess.Popen(
            [sys.executable, "walmart_sheet_updater.py", str(start_row), str(end_row)],
            stdout=open(LOG_FILE, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )

        st.session_state.process = process
        st.session_state.running = True
        st.session_state.logs = read_logs()

else:
    if st.sidebar.button("⏹ Stop Update"):
        if st.session_state.process:
            st.session_state.process.terminate()
        st.session_state.running = False
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n🛑 Update stopped by user.\n")

# --- Live Log Viewer ---
st.subheader("🧠 Live Logs")
st.session_state.logs = read_logs()

st.text_area(
    "Process Logs",
    st.session_state.logs,
    height=400,
    key="log_view",
)

# --- Process Completion Check ---
if st.session_state.running and st.session_state.process:
    process = st.session_state.process
    retcode = process.poll()

    if retcode is not None:  # means process finished
        st.session_state.running = False
        if retcode == 0:
            st.success(f"✅ Walmart Sheet successfully updated for rows {start_row}-{end_row}!")
        else:
            st.error(f"❌ Update failed (exit code {retcode}). Check logs below.")
