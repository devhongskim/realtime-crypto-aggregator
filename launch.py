import subprocess
import time
import sys
import os
import shutil
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Launch crypto streaming pipeline.")
parser.add_argument("--clean", action="store_true", help="Wipe old checkpoints and state files for a fresh start.")
args = parser.parse_args()

root_dir = Path(__file__).resolve().parent

if args.clean:
    print("🧹 Cleaning up old local Spark checkpoints and temp files (--clean flag detected)...")
    dirs_to_clean = [
        root_dir / "spark_job" / "checkpoint",
        root_dir / "spark_job" / "_checkpoint",
        root_dir / "spark_job" / "spark-warehouse",
    ]
    for d in dirs_to_clean:
        if d.exists() and d.is_dir():
            try:
                shutil.rmtree(d)
                print(f"   Deleted: {d.name}")
            except Exception as e:
                print(f"   ⚠️ Could not delete {d.name}: {e}")
else:
    print("🔄 Preserving existing checkpoints for state recovery (use --clean to wipe).")

print("🐳 Step 1: Starting Docker Compose (Kafka)...")
try:
    subprocess.run(["docker", "compose", "up", "-d"], cwd=root_dir, check=True)
except subprocess.CalledProcessError:
    print("❌ Error starting Docker. Make sure Docker Desktop is open!")
    sys.exit(1)

print("⏳ Waiting 5 seconds for Kafka broker to settle...")
time.sleep(5)

print("🚀 Step 2: Launching Coinbase Producer in the background...")
producer_script = root_dir / "producer" / "coinbase_producer.py"
producer_process = subprocess.Popen(["python", str(producer_script)], cwd=root_dir)

print("⏳ Waiting 3 seconds for producer to start streaming...")
time.sleep(3)

print("⚡ Step 3: Launching Spark in a dedicated PowerShell window...")

packages = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

spark_dir = root_dir / "spark_job"
env_file = root_dir / ".env"

ps_command = (
    f"cd '{spark_dir}'; "
    f"Get-Content '{env_file}' | ForEach-Object {{ "
    f"if ($_ -match '^\\s*([^#][^=]*)=(.*)$') {{ "
    f"[System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim()) "
    f"}} }}; "
    f"spark-submit --packages {packages} "
    f"--conf spark.driver.host=127.0.0.1 "
    f"--conf spark.driver.bindAddress=127.0.0.1 "
    f"--conf spark.driver.memory=2g "
    f"streaming_job.py"
)

subprocess.Popen(["start", "powershell", "-NoExit", "-Command", ps_command], shell=True)

print("\n🎉 All systems launched!")
print("📊 Grafana dashboard: http://localhost:3000 (Crypto Pipeline folder)")
print("💡 Press Ctrl+C in this main window whenever you want to stop the producer and Docker.")
print("💡 Reminder: If Spark fails to bind to 127.0.0.1, your network stack may still be settling after a fresh boot. Wait a moment and try again.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Shutting down producer...")
    producer_process.terminate()
    print("👋 Clean shutdown complete.")
