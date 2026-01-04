# -*- coding: utf-8 -*-
"""
DRT Pipeline with:
- Skip Raw files
- Clean renaming
- Parallel conversion
- TQDM progress bar
- Logging
- SHA-256 hash deduplication
"""

import os
import re
import json
import shutil
import hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime

# =========================================================
# USER SETTINGS
# =========================================================
current_density = "500"   # e.g. "50", "200", "400"
root_dir = r"Y:\5900\HydrogenTechFuelCellsGroup\CO2R\Nhan P\Experiments\CO2 Cell Testing\TS2\2NP48_conditioning 0p3 slpm\Compare Anolyte Concentration"
# =========================================================

prefix = f"PWRGEIS_{current_density}mA"
output_folder = os.path.join(root_dir, f"DRT_{current_density}mA")
os.makedirs(output_folder, exist_ok=True)

# =========================================================
# LOGGING
# =========================================================
log_path = os.path.join(output_folder, "DRT_log.txt")
dedup_path = os.path.join(output_folder, "DRT_hash_registry.json")

log_lines = []
def log(msg):
    print(msg)
    log_lines.append(msg)


# =========================================================
# LOAD / INIT HASH REGISTRY
# =========================================================
if os.path.exists(dedup_path):
    with open(dedup_path, "r") as fp:
        hash_registry = json.load(fp)
else:
    hash_registry = {}   # hash → csv_filename


# =========================================================
# HELPERS
# =========================================================
def sha256_file(path, block_size=65536):
    """Compute SHA-256 hash for deduplication."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()


def read_DTA(path):
    table_data = []
    in_table = False

    with open(path, encoding='windows-1252') as fp:
        for line in fp:
            if 'ZCURVE' in line:
                in_table = True
            elif in_table and line.strip() and not line.startswith('#'):
                table_data.append(line.strip().split('\t'))

    if not in_table:
        raise ValueError("ZCURVE table not found in file")

    table_data = [row for row in table_data if not any(x.startswith('#') for x in row)]
    df = pd.DataFrame(table_data[1:], columns=table_data[0]).astype(float)
    return df


def extract_keywords(df):
    keys = ["Freq", "Zreal", "Zimag"]
    return df[[col for col in keys if col in df.columns]].copy()


def clean_csv_name(folder_name):
    """Final CSV name: PWRGEIS_xxmA_<folder name>.csv"""
    safe_folder = folder_name.replace("/", "_").replace("\\", "_")
    return f"{prefix}_{safe_folder}.csv"


# =========================================================
# PARALLEL WORKER
# =========================================================
def process_file(task):
    renamed_path, folder_name = task
    filename = os.path.basename(renamed_path)

    # ----------------------------------------------------
    # Compute hash for deduplication
    # ----------------------------------------------------
    file_hash = sha256_file(renamed_path)

    if file_hash in hash_registry:
        return f"⏭ SKIPPED (duplicate hash): {filename}"

    # ----------------------------------------------------
    # Convert to CSV
    # ----------------------------------------------------
    try:
        df_raw = read_DTA(renamed_path)
        df_proc = extract_keywords(df_raw)

        csv_filename = clean_csv_name(folder_name)
        csv_path = os.path.join(output_folder, csv_filename)

        df_proc.to_csv(csv_path, index=False, header=False)

        # Save hash to registry
        hash_registry[file_hash] = csv_filename

        return f"✔ Converted: {filename} → {csv_filename}"

    except Exception as e:
        return f"❌ ERROR converting {filename}: {e}"


# =========================================================
# STEP 1 — SEARCH & RENAME
# =========================================================
log("\n========= SEARCHING & RENAMING =========\n")

tasks = []

for foldername, subdirs, files in os.walk(root_dir):
    if foldername == output_folder:
        continue

    folder_label = os.path.basename(foldername)

    for file in files:
        if not file.lower().endswith(".dta"):
            continue
        if file.lower().endswith("_raw.dta"):
            continue
        if not file.startswith(prefix):
            continue

        src_path = os.path.join(foldername, file)

        # Clean rename for DRT folder
        new_filename = f"{prefix}_{folder_label}.dta"
        renamed_path = os.path.join(output_folder, new_filename)

        shutil.copy2(src_path, renamed_path)
        log(f"Copied: {src_path} → {renamed_path}")

        tasks.append((renamed_path, folder_label))


# =========================================================
# STEP 2 — PARALLEL CONVERSION
# =========================================================
log("\n========= PARALLEL: DTA → CSV =========\n")

max_workers = min(32, os.cpu_count() * 2)

results = []
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_file, task) for task in tasks]

    for f in tqdm(as_completed(futures), total=len(futures), desc="Converting"):
        results.append(f.result())

for r in results:
    log(r)


# =========================================================
# SAVE LOG + HASH REGISTRY
# =========================================================
log("\nSaving log and hash table...")

with open(log_path, "w", encoding="utf-8") as fp:
    fp.write("\n".join(log_lines))

with open(dedup_path, "w", encoding="utf-8") as fp:
    json.dump(hash_registry, fp, indent=2)

log("\n✔ ALL DONE — Parallel, Deduplicated, Logged ✔\n")
log(f"Log saved:  {log_path}")
log(f"Hash file:  {dedup_path}")
