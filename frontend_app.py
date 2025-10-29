import streamlit as st
import subprocess
import os
import sys
import signal

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

# --- Auto-refresh every few seconds to show live logs ---
refresh_rate = 20  # seconds
st.markdown(
    f"""
    <meta http-equiv="refresh" content="{refresh_rate}">
    """,
    unsafe_allow_html=True,
)

# --- Helper to read logs safely ---
def read_logs():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return "No logs yet."

# --- Sidebar Controls ---
st.sidebar.header(" Controls")
start_row = st.sidebar.number_input("Start Row", min_value=2, value=2, step=1)
end_row = st.sidebar.number_input("End Row", min_value=start_row, value=start_row, step=1)
st.sidebar.markdown("---")

# --- Action Buttons (always visible) ---
col1, col2 = st.sidebar.columns(2)
start_clicked = col1.button("Start", use_container_width=True)
stop_clicked = col2.button("Stop", use_container_width=True)

# --- Handle Start ---
if start_clicked and not st.session_state.running:
    # Clear old log file
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"Starting Walmart Sheet Updater for rows {start_row}-{end_row}...\n")

    process = subprocess.Popen(
        [sys.executable, "walmart_sheet_updater.py", str(start_row), str(end_row)],
        stdout=open(LOG_FILE, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )

    st.session_state.process = process
    st.session_state.running = True
    st.session_state.logs = read_logs()
    st.success(f" Started Walmart Sheet Updater for rows {start_row}-{end_row}!")

# --- Handle Stop ---
if stop_clicked:
    if st.session_state.process:
        try:
            os.kill(st.session_state.process.pid, signal.SIGTERM)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write("\nUpdate stopped by user.\n")
            st.session_state.running = False
            st.session_state.process = None
            st.warning("Process stopped by user.")
        except Exception as e:
            st.error(f"Failed to stop process: {e}")
    else:
        st.warning(" No active process to stop.")

# --- Live Logs Viewer ---
st.subheader(" Live Logs")
st.session_state.logs = read_logs()
st.text_area("Process Logs", st.session_state.logs, height=400, key="log_view")

# --- Check if process finished automatically ---
if st.session_state.running and st.session_state.process:
    retcode = st.session_state.process.poll()
    if retcode is not None:  # Process finished
        st.session_state.running = False
        if retcode == 0:
            st.success(f"Walmart Sheet successfully updated for rows {start_row}-{end_row}!")
        else:
            st.error(f"Update failed (exit code {retcode}). Check logs below.")
        st.session_state.process = None
