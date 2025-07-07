import os
import pandas as pd
import matplotlib.pyplot as plt
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils.dataframe import dataframe_to_rows

# Input and output directories
spreadsheet_dir = "minio-report-tracker/spreadsheet"
output_dir = "minio-report-tracker/xlxs"

os.makedirs(output_dir, exist_ok=True)

def process_csv_file(csv_file):
    df = pd.read_csv(csv_file)

    # Clean empty columns (from padding logic)
    df = df.loc[:, ~df.columns.str.match(r'^\s*$')]

    # Convert Date column to datetime
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce', dayfirst=True)
    df = df.dropna(subset=["Date"])

    # Convert numeric columns
    for col in ["T", "P", "S", "F", "I", "KI"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def generate_graphs(df, module_name, output_path):
    module_df = df[df["Module"] == module_name].sort_values("Date")
    plt.figure(figsize=(10, 5))

    plt.plot(module_df["Date"], module_df["T"], label="Total", color="blue", marker='o')
    plt.plot(module_df["Date"], module_df["P"], label="Passed", color="green", marker='o')
    plt.plot(module_df["Date"], module_df["F"], label="Failed", color="red", marker='o')

    plt.title(f"Trend for Module: {module_name}")
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    graph_path = os.path.join(output_path, f"{module_name}_trend.png")
    plt.savefig(graph_path)
    plt.close()

    return graph_path

def create_excel_report(df, graphs, output_file):
    wb = Workbook()

    # Sheet 1: Data
    ws_data = wb.active
    ws_data.title = "Module Data"
    for row in dataframe_to_rows(df, index=False, header=True):
        ws_data.append(row)

    # Sheet 2: Graphs
    ws_graphs = wb.create_sheet(title="Module Graphs")
    row_pos = 1

    for module_name, graph_file in graphs:
        if os.path.exists(graph_file):
            img = Image(graph_file)
            img.width = 800
            img.height = 400
            ws_graphs.add_image(img, f"A{row_pos}")
            row_pos += 22  # spacing

    wb.save(output_file)

# Main logic
for filename in os.listdir(spreadsheet_dir):
    if filename.endswith(".csv"):
        csv_path = os.path.join(spreadsheet_dir, filename)
        df = process_csv_file(csv_path)

        if df.empty:
            print(f"⚠️ Skipping empty or invalid file: {filename}")
            continue

        temp_graph_dir = "temp_graphs"
        os.makedirs(temp_graph_dir, exist_ok=True)

        graphs = []
        for module in df["Module"].dropna().unique():
            graph_path = generate_graphs(df, module, temp_graph_dir)
            graphs.append((module, graph_path))

        output_excel_path = os.path.join(output_dir, filename.replace(".csv", ".xlsx"))
        create_excel_report(df, graphs, output_excel_path)
        print(f"✅ Excel report saved: {output_excel_path}")

        # Clean up graph images
        for _, gpath in graphs:
            if os.path.exists(gpath):
                os.remove(gpath)

        os.rmdir(temp_graph_dir)
