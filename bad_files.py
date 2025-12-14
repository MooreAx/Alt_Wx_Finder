#!/usr/bin/env python3
from pathlib import Path

# Folder containing your files
data_folder = Path("data")

bad_files = []

for file in sorted(data_folder.glob("*")):
    print(f"Running {file}...")
    if not file.is_file():
        continue
    try:
        file.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        bad_files.append((file.name, str(e)))

if bad_files:
    print("Files with UTF-8 errors:")
    for fname, err in bad_files:
        print(f"  {fname}: {err}")
else:
    print("All files appear to be valid UTF-8!")
