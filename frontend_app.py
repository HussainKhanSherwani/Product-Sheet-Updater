import streamlit as st
import subprocess
import os
import sys
import time

LOG_FILE = "scraper.log"

st.set_page_config(page_title="Walmart Sheet Updater", layout="wide")
st.title("🧾 Walmart Product Sheet Updater")

# --- Initialize session state ---
if "running" not in st.session_state:
    st.session_state.running = False
if "logs" not in st.session_state:
    st.session_state.logs = ""
if "process" not in st.session_state:
    st.session_state.process = None

# --- Helper: read logs safely ---
def read_logs():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return "No logs yet."

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Controls")

start_row = st.sidebar.number_input("Start Row", min_value=2, value=2, step=1)
end_row = st.sidebar.number_input("End Row", min_value=start_row, value=start_row, step=1)

# --- Start / Stop logic ---
if not st.session_state.running:
    if st.sidebar.button("▶️ Start Update"):
        # Ensure no leftover log file
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Walmart sheet updater started for rows {start_row}-{end_row}...\n")

        process = subprocess.Popen(
            [sys.executable, "walmart_sheet_updater.py", str(start_row), str(end_row)],
            stdout=open(LOG_FILE, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )

        st.session_state.process = process
        st.session_state.running = True
        st.rerun()
else:
    if st.sidebar.button("⏹ Stop Update"):
        if st.session_state.process:
            st.session_state.process.terminate()
        st.session_state.running = False
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🛑 Update stopped by user.\n")
        st.rerun()

# --- Live Log Viewer ---
st.subheader("🧠 Live Logs")

log_box = st.empty()
main_stop_btn = st.empty()

if st.session_state.running:
    # Show an inline Stop button too
    if main_stop_btn.button("⏹ Stop Update", type="secondary"):
        if st.session_state.process:
            st.session_state.process.terminate()
        st.session_state.running = False
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🛑 Update stopped by user.\n")
        st.rerun()

    # Show current logs
    logs = read_logs()
    log_box.text_area("Process Logs", logs, height=400, key="log_display", disabled=True)

    # Check if process ended
    if st.session_state.process and st.session_state.process.poll() is not None:
        st.session_state.running = False
        st.rerun()
    else:
        # Auto-refresh logs every few seconds
        time.sleep(5)
        st.rerun()

else:
    # Static log view when not running
    logs = read_logs()
    log_box.text_area("Process Logs", logs, height=400, key="log_display", disabled=True)

# --- Completion Feedback ---
if st.session_state.process and not st.session_state.running:
    retcode = st.session_state.process.poll()
    if retcode == 0:
        st.success(f"✅ Walmart Sheet successfully updated for rows {start_row}-{end_row}!")
    elif retcode is not None:
        st.error(f"❌ Update failed (exit code {retcode}). Check logs below.")
