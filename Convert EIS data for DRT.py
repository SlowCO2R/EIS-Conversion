# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 15:22:52 2025

@author: tpham + ChatGPT
"""
#Compile EIS data into csv without header for DRT
import pandas as pd
import math
import matplotlib
import numpy as np
import os
import matplotlib.pyplot as plt
import re
from scipy.optimize import curve_fit
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
from scipy.optimize import fsolve

#% Settings
folder_path = r'Y:\5900\HydrogenTechFuelCellsGroup\CO2R\Nhan P\Experiments\CO2 Cell Testing\TS2\2NP57_Measure Capacitance\Test BOT'
KEYWORDS = ['Freq', 'Zreal', 'Zimag']
EXPORT_FOLDER = os.path.join(folder_path, "Processed_CSVs")
os.makedirs(EXPORT_FOLDER, exist_ok=True)

#% Read DTA file
def read_DTA(path):
    table_data = []
    in_table = False

    with open(path, encoding='windows-1252') as fp:
        for i, line in enumerate(fp):
            if 'ZCURVE' in line:
                in_table = True
                starting_line = i + 4
            elif in_table and line.strip() and not line.startswith('#'):
                table_data.append(line.strip().split('\t'))

    if not in_table:
        print(f"[{os.path.basename(path)}] ❌ ZCURVE table not found in file.")
        raise ValueError("ZCURVE table not found in DTA file")

    table_data_cleaned = [row for row in table_data if not any(entry.startswith('#') for entry in row)]
    
    try:
        df = pd.DataFrame(table_data_cleaned[1:], columns=table_data_cleaned[0]).astype(float)
    except ValueError as e:
        print(f"[{os.path.basename(path)}] Data causing conversion error:")
        print(f"Data causing the error: {table_data_cleaned}")
        raise

    print(f"[{os.path.basename(path)}] ✅ ZCURVE table extracted: {df.shape[0]} rows × {df.shape[1]} cols")
    return df

#% Extract specific columns
def extract_keywords(df: pd.DataFrame, keywords: list) -> pd.DataFrame:
    missing = set(keywords) - set(df.columns)
    if missing:
        print(f"⚠️ Missing columns: {missing}")
    present = [col for col in keywords if col in df.columns]

    df_filtered = df[present].copy()

    return df_filtered

#% Main loop
def main():
    raw_data = {}
    for filename in os.listdir(folder_path):
        if not filename.lower().endswith('.dta'):
            continue
        path = os.path.join(folder_path, filename)
        try:
            raw_df = read_DTA(path)
            raw_data[filename] = raw_df
        except ValueError as e:
            print(f"[{filename}] Skipped: {e}")

    processed_data = {}
    for filename, df in raw_data.items():
        print(f"\nProcessing keywords for {filename}:")
        proc_df = extract_keywords(df, KEYWORDS)
        print(f"[{filename}] → Columns kept: {list(proc_df.columns)} | Shape: {proc_df.shape}")
        processed_data[filename] = proc_df

        # Export to CSV without header or index
        csv_filename = os.path.splitext(filename)[0] + ".csv"
        csv_path = os.path.join(EXPORT_FOLDER, csv_filename)
        proc_df.to_csv(csv_path, index=False, header=False)
        print(f"[{filename}] ⬇ Exported to: {csv_path}")

    return processed_data

#% Run
if __name__ == '__main__':
    all_data_dict = main()
