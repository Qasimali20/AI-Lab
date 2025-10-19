import os
from datetime import datetime

os.makedirs("logs", exist_ok=True)
with open("logs/pipeline.log", "a", encoding="utf-8") as f:  # specify UTF-8
    f.write(f"[{datetime.utcnow()}] ✅ Pipeline completed successfully\n")

print("🪶 Logged pipeline status to logs/pipeline.log")
