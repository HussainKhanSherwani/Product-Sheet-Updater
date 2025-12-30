import streamlit as st
import subprocess
import os
import sys
import signal
import re

LOG_FILE = "scraper.log"
LOCK_FILE = "start.txt"

st.set_page_config(page_title="Walmart Sheet Updater", layout="wide")
st.title("🧾 Walmart Product Sheet Updater")

# --- Initialize session state ---
if "running" not in st.session_state:
    st.session_state.running = False
if "logs" not in st.session_state:
    st.session_state.logs = ""
if "process" not in st.session_state:
    st.session_state.process = None

# --- Auto-refresh logs ---
refresh_rate = 60
st.markdown(f"""<meta http-equiv="refresh" content="{refresh_rate}">""", unsafe_allow_html=True)

# --- Helper to read logs safely ---
def read_logs():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return "No logs yet."

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Configuration")

# 1. Mode Selection
mode = st.sidebar.radio("Select Mode:", ["Range Mode (Start-End)", "List Mode (Specific Rows)"])

final_cmd_args = []

if mode == "Range Mode (Start-End)":
    start_row = st.sidebar.number_input("Start Row", min_value=2, value=2, step=1)
    end_row = st.sidebar.number_input("End Row", min_value=start_row, value=start_row, step=1)
    st.sidebar.info(f"Will scrape rows: {start_row} to {end_row}")
    final_cmd_args = [str(start_row), str(end_row)]

else:
    st.sidebar.markdown("### Paste Row Numbers")
    raw_input = st.sidebar.text_area("Enter rows (comma or new line separated)", "3, 5, 10\n20")
    
    # Parse input: split by comma, newline, or space
    if raw_input:
        # regex split by comma, newline, pipe, space
        tokens = re.split(r'[,\s\n|]+', raw_input)
        # filter only digits
        valid_rows = [t for t in tokens if t.isdigit()]
        
        if valid_rows:
            st.sidebar.success(f"Found {len(valid_rows)} rows: {', '.join(valid_rows[:5])}...")
            # Create the comma-separated string for backend
            rows_arg = ",".join(valid_rows)
            final_cmd_args = ["list", rows_arg]
        else:
            st.sidebar.warning("No valid numbers found.")

st.sidebar.markdown("---")

# --- Lock Check ---
is_locked = os.path.exists(LOCK_FILE) or st.session_state.running

# --- Buttons ---
col1, col2 = st.sidebar.columns(2)
start_clicked = col1.button("Start", use_container_width=True, disabled=is_locked)
stop_clicked = col2.button("Stop", use_container_width=True)

# --- START Action ---
if start_clicked and not st.session_state.running:
    if not final_cmd_args:
        st.error("Please configure rows first.")
    else:
        # Create lock
        with open(LOCK_FILE, "w") as lock:
            lock.write("running")

        # Clear logs
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"Starting Scraper in {mode}...\n")

        # Launch Backend
        # We pass the final_cmd_args we built above
        cmd = [sys.executable, "walmart_sheet_updater.py"] + final_cmd_args
        
        process = subprocess.Popen(
            cmd,
            stdout=open(LOG_FILE, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )

        st.session_state.process = process
        st.session_state.running = True
        st.session_state.logs = read_logs()
        st.rerun()

# --- STOP Action ---
if stop_clicked:
    if st.session_state.process:
        try:
            os.kill(st.session_state.process.pid, signal.SIGTERM)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write("\n⚠️ Stopped by user.\n")
            st.session_state.running = False
            st.session_state.process = None
            st.warning("Stopped.")
        except Exception as e:
            st.error(f"Stop failed: {e}")
    else:
        st.warning("Not running.")

# --- Logs View ---
st.subheader("📝 Live Logs")
st.session_state.logs = read_logs()
st.text_area("Logs", st.session_state.logs, height=500)

# --- Auto Check Status ---
if st.session_state.running and st.session_state.process:
    retcode = st.session_state.process.poll()
    if retcode is not None:
        st.session_state.running = False
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        
        if retcode == 0:
            st.success("✅ Job Finished Successfully!")
        else:
            st.error(f"❌ Job Failed (Code {retcode})")
        
        st.session_state.process = None
        st.rerun()