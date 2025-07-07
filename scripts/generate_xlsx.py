import os
import pandas as pd
import matplotlib.pyplot as plt
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils.dataframe import dataframe_to_rows

def load_and_normalize_data(input_file):
    rows = []
    with open(input_file, 'r') as f:
        for line in f:
            if "Date" in line or not line.strip():
                continue
            parts = [p.strip() for p in line.strip().split(',') if p.strip()]
            for i in range(0, len(parts), 8):
                if i + 7 < len(parts):
                    row = parts[i:i + 8]
                    rows.append(row)

    df = pd.DataFrame(rows, columns=["Date", "Module", "T", "P", "S", "F", "I", "KI"])
    df = df[df["Date"].str.contains(r'\d{1,2}-[A-Za-z]+-\d{4}', na=False)]
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%B-%Y")
    for col in ["T", "P", "S", "F", "I", "KI"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def get_output_folder(df, alias):
    start_date = df["Date"].min().strftime("%d-%B-%Y")
    end_date = df["Date"].max().strftime("%d-%B-%Y")
    folder_name = f"xlxs/api-test-rig-{alias}-{start_date}_to_{end_date}"
    os.makedirs(folder_name, exist_ok=True)
    return folder_name

def generate_graphs(df, output_dir):
    modules = df["Module"].unique()
    graph_files = []

    for module in modules:
        module_df = df[df["Module"] == module].sort_values("Date")

        plt.figure(figsize=(10, 5))
        plt.plot(module_df["Date"], module_df["T"], label="Total", linewidth=2, color='blue', marker='o')
        plt.plot(module_df["Date"], module_df["P"], label="Passed", linewidth=2.5, color='green', marker='o')
        plt.plot(module_df["Date"], module_df["F"], label="Failed", linewidth=2.5, color='red', marker='o')
        plt.title(f"Trend for Module: {module}", fontsize=14)
        plt.xlabel("Date", fontsize=12)
        plt.ylabel("Count", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.legend()
        plt.tight_layout()

        file_path = os.path.join(output_dir, f"{module}_trend.png")
        plt.savefig(file_path)
        graph_files.append((module, file_path))
        plt.close()

    return graph_files

def export_to_excel(df, graph_files, output_dir, alias):
    wb = Workbook()
    ws_data = wb.active
    ws_data.title = "Module Data"

    for row in dataframe_to_rows(df, index=False, header=True):
        ws_data.append(row)

    ws_charts = wb.create_sheet(title="Module Graphs")
    row_pos = 1
    for module, image_path in graph_files:
        if not os.path.exists(image_path):
            continue
        img = Image(image_path)
        img.width = 800
        img.height = 400
        cell = f"A{row_pos}"
        ws_charts.add_image(img, cell)
        row_pos += 22

    output_file = os.path.join(output_dir, f"{alias}_module_report.xlsx")
    wb.save(output_file)
    return output_file

# Entry point
spreadsheet_dir = "spreadsheet"
os.makedirs("xlxs", exist_ok=True)

for file in os.listdir(spreadsheet_dir):
    if file.endswith(".csv"):
        alias = file.replace(".csv", "")
        csv_path = os.path.join(spreadsheet_dir, file)
        df = load_and_normalize_data(csv_path)
        output_dir = get_output_folder(df, alias)
        graph_files = generate_graphs(df, output_dir)
        xlsx_file = export_to_excel(df, graph_files, output_dir, alias)
        print(f"✅ XLSX generated: {xlsx_file}")
