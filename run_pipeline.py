# run_pipeline.py
import yaml
import subprocess
import time
import os
import sys
from datetime import datetime

os.makedirs("logs", exist_ok=True)

def run_step(name, script, desc):
    print(f"\n🔹 Running step: {name} — {desc}")
    try:
        subprocess.run([sys.executable, script], check=True)
        print(f"✅ Completed: {name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error in step {name}: {e}")
        with open("logs/error.log", "a") as f:
            f.write(f"[{datetime.utcnow()}] {name} failed: {e}\n")


def run_pipeline():
    with open("mcp_schedule.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    steps = cfg["pipeline"]["steps"]
    hours = cfg["pipeline"]["schedule"]["interval_hours"]
    print(f"🚀 Starting MCP pipeline. Interval: {hours} hours")

    while True:
        print(f"\n🕒 New pipeline run at {datetime.utcnow()} UTC")
        for step in steps:
            run_step(step["name"], step["script"], step["description"])
        print(f"✅ Pipeline completed at {datetime.utcnow()} UTC")
        print(f"💤 Sleeping for {hours} hours...\n")
        time.sleep(hours * 3600)

if __name__ == "__main__":
    run_pipeline()
