import os
import subprocess
import time
import signal

def restart_fastapi():
    print("🔄 Restarting FastAPI service...")

    # Try to find and terminate existing uvicorn processes
    try:
        out = subprocess.check_output("tasklist", shell=True).decode()
        if "uvicorn.exe" in out or "uvicorn" in out:
            print("🛑 Found running FastAPI process. Killing it...")
            subprocess.call("taskkill /F /IM uvicorn.exe", shell=True)
            time.sleep(3)
    except Exception:
        pass

    # Restart the API service
    cmd = "uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload"
    subprocess.Popen(cmd, shell=True)
    print("✅ FastAPI restarted successfully.")

if __name__ == "__main__":
    restart_fastapi()
