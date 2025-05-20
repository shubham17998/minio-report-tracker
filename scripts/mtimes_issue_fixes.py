import subprocess, json, re
import pandas as pd

def fetch_reports(folder, alias, bucket):
    # 1) grab all lines
    out = subprocess.getoutput(f"mc ls --json {alias}/{bucket}/{folder}/")
    items = [json.loads(l) for l in out.splitlines() if l.strip()]
    print(f"🔍 Found {len(items)} items in {folder}")

    # 2) filter to full‐reports that also have 'mtime'
    full = [i for i in items if 'full-report' in i.get('key', '') and 'mtime' in i]
    print(f"✅ {len(full)} full-reports with mtime in {folder}")
    return full

def extract(rows, start, count, folder):
    data = []
    for rec in rows[start:start+count]:
        fn = rec['key']
        m = re.search(
            r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)",
            fn
        )
        if not m:
            print(f"⚠️ Skipped unmatched filename: {fn}")
            continue
        T, P, S, F, I, KI = m.groups()
        if folder == 'masterdata':
            lang_match = re.search(r"masterdata-([a-z]{3})", fn)
            lang = lang_match.group(1) if lang_match else "unk"
            mod = f"{folder}-{lang}"
        else:
            mod = folder
        data.append([mod, T, P, S, F, I, KI])
    print(f"📦 Extracted {len(data)} records from {folder}")
    return data

# Configuration
alias = "myminio"
bucket = "automation"
folders = ["auth", "idrepo", "masterdata", "partner", "pms", "prereg", "resident"]

rep1, rep2 = [], []

# Process each folder
for fld in folders:
    rows = fetch_reports(fld, alias, bucket)
    if fld == 'masterdata':
        rep1 += extract(rows, 0, 6, fld)    # most recent 1–6
        rep2 += extract(rows, 6, 6, fld)    # next 7–12
    else:
        rep1 += extract(rows, 0, 1, fld)    # most recent 1
        rep2 += extract(rows, 1, 1, fld)    # second most recent

# Debug: Print data lengths
print(f"📝 Total rows in rep1: {len(rep1)}")
print(f"📝 Total rows in rep2: {len(rep2)}")

# Build DataFrames
cols = ["Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(rep1, columns=cols)
df2 = pd.DataFrame(rep2, columns=cols)

# Insert two blank separator columns into df1
df1[""], df1[""] = "", ""

# Concatenate side-by-side, aligning rows by position
df_wide = pd.concat([df1.reset_index(drop=True),
                     df2.reset_index(drop=True)],
                    axis=1)

# Write to CSV
df_wide.to_csv("reports_wide.csv", index=False)
print("✅ reports_wide.csv written with two tables correctly aligned.")
