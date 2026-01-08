import subprocess

subprocess.run(["python3", "/Workspace/Users/<user>/scripts/ingest_csv_to_s3.py"])
subprocess.run(["python3", "/Workspace/Users/<user>/scripts/ingest_s3_to_delta.py"])