import subprocess
import time
import os
import webbrowser
import threading

def launch_watcher():
    return subprocess.Popen(
        ["python", "watchers/h1b_file_watcher.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

def launch_streamlit():
    return subprocess.Popen(
        ["streamlit", "run", "app/streamlit_app.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

def monitor_streamlit_and_open_browser(proc):
    for line in proc.stdout:
        print("[Streamlit]", line.strip())
        if "Local URL:" in line:
            # Extract URL from line
            url = line.split("Local URL:")[-1].strip()
            print(f"🌐 Opening browser at {url}")
            webbrowser.open(url)
            break

def run():
    print("🔁 Starting H1B Job Matcher System...")

    watcher = launch_watcher()
    print("📂 File watcher started.")

    time.sleep(2)

    streamlit = launch_streamlit()
    print("🚀 Streamlit app started.")

    # Launch thread to monitor and open browser
    threading.Thread(target=monitor_streamlit_and_open_browser, args=(streamlit,), daemon=True).start()

    try:
        streamlit.wait()
    except KeyboardInterrupt:
        print("🛑 Terminating...")
        watcher.terminate()
        streamlit.terminate()
        watcher.wait()
        streamlit.wait()
        print("✅ Clean exit.")

if __name__ == "__main__":
    run()
