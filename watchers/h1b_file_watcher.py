import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess

INCOMING_FOLDER = "data/incoming_h1b"
TARGET_FILENAME = "data/employer_data.xlsx"
PROCESSED_FLAG = "data/.reload_flag"

class H1BExcelHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith(".xlsx"):
            try:
                print(f"📥 New Excel file detected: {event.src_path}")
                # Move to incoming folder with timestamp to avoid overwrite
                base_name = os.path.basename(event.src_path)
                target_path = os.path.join(INCOMING_FOLDER, base_name)
                os.replace(event.src_path, target_path)
                print(f"📂 Moved to: {target_path}")

                # Run the preprocessing script
                print("⚙️ Running preprocessing script...")
                subprocess.run(["python", "spark_pipeline/preprocess.py"], check=True)

                # Touch reload flag for Streamlit
                with open(PROCESSED_FLAG, "w") as f:
                    f.write(str(time.time()))

                print("✅ Done! Data reloaded and Streamlit will refresh.")
            except Exception as e:
                print(f"❌ Error during file processing: {e}")

def start_file_watcher():
    print(f"👀 Watching {INCOMING_FOLDER} for new .xlsx files...")
    event_handler = H1BExcelHandler()
    observer = Observer()
    observer.schedule(event_handler, path=INCOMING_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_file_watcher()
