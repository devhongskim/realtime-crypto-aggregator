import subprocess
import time
import sys
import os
from pathlib import Path

root_dir = Path(__file__).resolve().parent

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

# Build a clean PowerShell script command block using raw strings/escaped regex
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

# Open a new PowerShell window running the command block
subprocess.Popen(["start", "powershell", "-NoExit", "-Command", ps_command], shell=True)

print("\n🎉 All systems launched! Producer is running in the background, and Spark has its own window.")
print("💡 Press Ctrl+C in this main window whenever you want to stop the producer and Docker.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Shutting down producer...")
    producer_process.terminate()
    print("👋 Clean shutdown complete.")