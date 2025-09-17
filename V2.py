# app.py
import streamlit as st
import pandas as pd
import time
import os
import io
import tempfile
import re
import zipfile
import shutil
import getpass
import pyodbc
from typing import List, Dict, Tuple, Optional

# ---------------- CONFIG ----------------
SERVER = r"taxrollstage-db"
DATABASE = "TaxrollStaging"
DRIVER = "{ODBC Driver 13 for SQL Server}"
CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"

BATCH_SIZE = 50000

CAD_TABLE = "tbl_cads_shiny"
CAD_CODE_COL = "cad_CADCode"
CAD_ID_COL = "cad_CADID"
CAD_STATE_COL = "cad_State"

# ---------------- Layouts ----------------
DUMP_LAYOUTS = {
    "Appraisal Info": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\Appraisal Export Layout - 8.0.30 2.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{6}_APPRAISAL_INFO\.TXT",
        "int_columns": {
            'prop_val_yr', 'land_hstd_val', 'land_non_hstd_val', 'imprv_hstd_val', 'imprv_non_hstd_val',
            'ag_use_val', 'appraised_val', 'assessed_val', 'market_value', 'omitted_imprv_hstd_val',
            'omitted_imprv_non_hstd_val', 'pp_late_interstate_allocation_val',
            'appraised_val_reflecting_productivity_loss', 'assessed_val_reflecting_productivity_loss',
            'ag_market', 'timber_market', 'timber_78_market', 'prop_id', 'geo_id', 'filler9',
            'ten_percent_cap', 'legal_acreage', 'timber_use', 'entity_agent_id', 'ca_agent_id',
            'arb_agent_id', 'rendition_fraud_penalty', 'rendition_penalty', 'nhs_cap_loss'
        },
        "base_table_name": "APPRAISAL_INFO"
    },
    "Appraisal Improvement Info": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_IMPROVEMENT_INFO.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{6}_APPRAISAL_IMPROVEMENT_INFO.TXT(?:\.TXT)?",
        "int_columns": {'prop_id', 'imprv_id', 'imprv_homesite_pct', 'imprv_val'},
        "base_table_name": "APPRAISAL_IMPROVEMENT_INFO"
    },
    "Appraisal Improvement Detail": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_IMPRV_DETAIL.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{6}_APPRAISAL_IMPROVEMENT_DETAIL\.TXT",
        "int_columns": {'prop_id', 'imprv_id', 'imprv_det_id', 'detail_val', 'imprv_det_area', 'imprv_det_val'},
        "base_table_name": "APPRAISAL_IMPROVEMENT_DETAIL"
    },
    "Appraisal Land Detail": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_LAND_DETAIL.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{6}_APPRAISAL_LAND_DETAIL\.TXT",
        "int_columns": {'prop_id', 'land_seg_id'},
        "numeric_columns": {
            "prop_val_yr",
            "size_acres",
            "size_square_feet",
            "effective_front",
            "effective_depth",
            "land_seg_mkt_val",
            "ag_value",
            "land_homesite_pct"
        },
        "base_table_name": "APPRAISAL_LAND_DETAIL"
    },
    "Appraisal Abstract Subdv": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_ABSTRACT_SUBDV.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{6}_APPRAISAL_ABSTRACT_SUBDV\.TXT",
        "int_columns": set(),
        "numeric_columns": set(),
        "base_table_name": "APPRAISAL_ABSTRACT_SUBDV"
    }
}

# --------------- Streamlit helpers ---------------
def init_state():
    if "rows" not in st.session_state:
        st.session_state.rows = [{"file": None, "county": "", "layout": "All"}]
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "tables_by_layout" not in st.session_state:
        st.session_state.tables_by_layout = {}  # layout -> [temp tables]
    if "tax_year" not in st.session_state:
        st.session_state.tax_year = "2026"

def log_to_ui(msg: str, module: str = "Main"):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    username = getpass.getuser()
    entry = f"[{ts}] {msg}"

    # Append to session logs for UI
    st.session_state.logs.append(entry)
    if len(st.session_state.logs) > 1000:
        st.session_state.logs = st.session_state.logs[-1000:]

    # Save log into SQL
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO TaxrollLogs (Timestamp, Username, Module, Message) VALUES (?, ?, ?, ?)",
                (ts, username, module, msg)
            )
            conn.commit()
    except Exception as e:
        print(f"Failed to insert log into DB: {e}")

    # Print to console too
    print(entry)

def render_logs():
    if st.session_state.get("logs"):
        log_text = "\n".join(st.session_state.logs[-50:])
        st.text_area("Live Log (Last 50 entries)", value=log_text, height=220)

# ---------------- DB helpers ----------------
def get_db_connection():
    return pyodbc.connect(CONN_STR)

def fetch_county_codes():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT {CAD_CODE_COL} FROM {CAD_TABLE} WHERE {CAD_STATE_COL}='TX' ORDER BY {CAD_CODE_COL}"
        )
        rows = [r[0] for r in cur.fetchall()]
    log_to_ui(f"Fetched {len(rows)} counties from DB")
    return rows

def get_cad_id_for_code(cad_code):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT TOP 1 {CAD_ID_COL} FROM {CAD_TABLE} WHERE {CAD_CODE_COL}=? AND {CAD_STATE_COL}='TX'",
            (cad_code,),
        )
        r = cur.fetchone()
        return r[0] if r else None

# ---------------- Type safety parsers ----------------
def safe_int(val, min_val=-2147483648, max_val=2147483647):
    try:
        if val is None:
            return None
        s = str(val).strip()
        if s == "":
            return None
        s = s.replace(",", "")
        if "." in s:
            s = s.split(".")[0]
        iv = int(s)
        if min_val <= iv <= max_val:
            return iv
    except:
        pass
    return None

def safe_numeric(val):
    try:
        if val is None:
            return None
        s = str(val).strip()
        if s == "":
            return None
        s = s.replace(",", "")
        return float(int(s))
    except:
        return None

def column_exists(conn, table_name, column_name):
    cur = conn.cursor()
    sql = "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?"
    cur.execute(sql, (table_name, column_name))
    return cur.fetchone() is not None

def add_column_if_missing(table_name, column_name, data_type="INT"):
    with get_db_connection() as conn:
        if column_exists(conn, table_name, column_name):
            log_to_ui(f"Column '{column_name}' already exists in {table_name}.")
            return True
        try:
            log_to_ui(f"Adding column '{column_name}' to {table_name} ...")
            cur = conn.cursor()
            cur.execute(f"ALTER TABLE {table_name} ADD [{column_name}] {data_type}")
            conn.commit()
            log_to_ui(f"Column '{column_name}' added.")
            return True
        except Exception as e:
            log_to_ui(f"ERROR adding column '{column_name}': {e}")
            return False

def update_table_set_column(conn, table_name, column_name, value):
    cur = conn.cursor()
    cur.execute(f"UPDATE {table_name} SET {column_name} = ?", (value,))
    conn.commit()
    cur.close()
    return True

# ---------------- Core loader ----------------
def process_zip_load(
    zip_path,
    layout_path,
    txt_pattern,
    int_columns,
    base_table_name,
    county_code=None,
    tax_year=None,
    numeric_columns=None
):
    if numeric_columns is None:
        numeric_columns = set()
    try:
        log_to_ui("Locating TXT file...")
        # Accept .zip or direct .TXT
        if zip_path.lower().endswith(".zip"):
            with zipfile.ZipFile(zip_path, 'r') as z:
                txt_files = [f for f in z.namelist() if re.fullmatch(txt_pattern, os.path.basename(f))]
                if not txt_files:
                    log_to_ui("TXT file not found in ZIP.")
                    return False
                txt_filename = sorted(txt_files, reverse=True)[0]
                extract_dir = os.path.dirname(zip_path)
                z.extract(txt_filename, path=extract_dir)
                txt_path = os.path.join(extract_dir, txt_filename)
        else:
            txt_path = zip_path

        # Load Excel layout
        layout_df = pd.read_excel(layout_path)
        layout_df.columns = [c.strip().lower() for c in layout_df.columns]
        field_col = next(c for c in layout_df.columns if 'field' in c and 'name' in c)
        start_col = next(c for c in layout_df.columns if c.startswith('start'))
        end_col = next(c for c in layout_df.columns if c.startswith('end'))
        layout_positions = [(c, int(s) - 1, int(e)) for c, s, e in
                            zip(layout_df[field_col], layout_df[start_col], layout_df[end_col])]
        field_names = [c for c, _, _ in layout_positions]

        # Read TXT lines
        with open(txt_path, "r", encoding="cp1252", errors="replace") as f:
            lines = f.readlines()

        # Parse rows
        rows = []
        for line in lines:
            row = []
            for c, start, end in layout_positions:
                val = line[start:end].strip()
                if c in int_columns:
                    row.append(safe_int(val))
                elif c in numeric_columns:
                    row.append(safe_numeric(val))
                else:
                    row.append(val if val else None)
            rows.append(row)

        # Table name with username
        username = getpass.getuser().replace(" ", "_")
        table_name = f"{base_table_name}_{username}"
        if county_code and tax_year:
            table_name = f"{base_table_name}_{county_code}_{username}_{tax_year}"

        # Create SQL table
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            col_defs = []
            for c in field_names:
                if c in int_columns:
                    col_defs.append(f"[{c}] INT")
                elif c in numeric_columns:
                    col_defs.append(f"[{c}] FLOAT")
                else:
                    col_defs.append(f"[{c}] NVARCHAR(MAX)")
            cur.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
            conn.commit()

        # Bulk insert
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        placeholders = ", ".join("?" for _ in field_names)
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        batch, total_inserted = [], 0
        for r in rows:
            batch.append(r)
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
                log_to_ui(f"Inserted {total_inserted} rows...")
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)
        cursor.close()
        conn.close()

        # NOTE: archive removed per requirement

        return True, table_name
    except Exception as e:
        log_to_ui(f"ERROR during load: {e}")
        return False

# ---------------- Consolidation ----------------
def consolidate_per_layout(tables_by_layout: Dict[str, List[str]]):
    """
    Consolidate tables per layout into final tables with username suffix,
    then drop the intermediate tables.
    """
    username = getpass.getuser().replace(" ", "_")
    with get_db_connection() as conn:
        cur = conn.cursor()
        for layout, tbls in tables_by_layout.items():
            if not tbls:
                continue
            log_to_ui(f"Consolidating {len(tbls)} tables for layout '{layout}' into a single table...")

            final_table = f"{DUMP_LAYOUTS[layout]['base_table_name']}_FINAL_{username}"
            union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tbls])

            # Drop final table if exists
            cur.execute(f"IF OBJECT_ID('{final_table}', 'U') IS NOT NULL DROP TABLE {final_table}")
            # Create final table
            cur.execute(f"SELECT * INTO {final_table} FROM ({union_sql}) as U")
            log_to_ui(f"Consolidation for '{layout}' complete: {final_table}")

            # Drop temp tables (SQL Server 2016 safe pattern)
            for t in tbls:
                try:
                    cur.execute(f"IF OBJECT_ID('{t}', 'U') IS NOT NULL DROP TABLE {t}")
                    log_to_ui(f"Dropped intermediate table: {t}")
                except Exception as e:
                    log_to_ui(f"Warning: could not drop {t} ({e})")
        conn.commit()
    log_to_ui("All consolidations complete. You can use the FINAL tables.")

# ---------------- Worker ----------------
def worker_full_flow(
    zip_path: str,
    dump_type: str,
    county_code: str,
    tax_year: str
) -> Optional[str]:
    """
    Runs a single layout load, sets CADID, returns created table name (or None).
    """
    try:
        log_to_ui(f"Starting workflow for '{dump_type}'")
        config = DUMP_LAYOUTS[dump_type]
        layout_path = config["layout_path"]
        txt_pattern = config["txt_pattern"]
        int_columns = config["int_columns"]
        numeric_columns = set(config.get("numeric_columns", set()))
        base_table_name = config["base_table_name"]

        res = process_zip_load(
            zip_path,
            layout_path,
            txt_pattern,
            int_columns,
            base_table_name,
            county_code,
            tax_year,
            numeric_columns=numeric_columns
        )
        if not res:
            log_to_ui("Load failed.")
            return None
        _, table_name = res

        cadid = get_cad_id_for_code(county_code)
        if cadid is None:
            log_to_ui(f"No CADID found for county '{county_code}'")
            return table_name  # Table created; CADID not set

        if not add_column_if_missing(table_name, CAD_ID_COL, data_type="INT"):
            log_to_ui("Failed to add CADID column.")
            return table_name

        with get_db_connection() as conn:
            update_table_set_column(conn, table_name, CAD_ID_COL, cadid)

        log_to_ui(f"Workflow completed successfully for '{dump_type}'.")
        return table_name
    except Exception as e:
        log_to_ui(f"Unexpected error: {e}")
        return None

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="Taxroll Consolidation Manager", layout="wide")
init_state()

st.title("Taxroll Consolidation Manager")
st.caption("ETL + CADID update + Per-layout consolidation (Streamlit UI)")

top1, top2, top3 = st.columns([1, 1, 2])
with top1:
    st.selectbox("Tax Year", ["2026", "2025", "2024", "2023"], key="tax_year")
with top2:
    # Fetch counties once (cache)
    @st.cache_data(show_spinner=False)
    def _get_counties():
        return fetch_county_codes()
    all_counties = _get_counties()
with top3:
    st.info("Add rows for multiple dumps. Each row = **Browse + County + Layout**. Max 30 rows.", icon="ℹ️")

left, right = st.columns([1.3, 1])

# ---------- LEFT: Rows (inputs) ----------
with left:
    st.subheader("Inputs")
    addcol, rmcol = st.columns([1,1])
    with addcol:
        if st.button("➕ Add Row", use_container_width=True):
            if len(st.session_state.rows) < 30:
                st.session_state.rows.append({"file": None, "county": "", "layout": "All"})
            else:
                st.warning("Maximum 30 rows reached.")
    with rmcol:
        if st.button("➖ Remove Last", use_container_width=True):
            if len(st.session_state.rows) > 1:
                st.session_state.rows.pop()

    # Render each row
    for idx, row in enumerate(st.session_state.rows):
        with st.container():
            st.markdown(f"**Row {idx+1}**")
            c1, c2, c3 = st.columns([1.4, 1, 1])
            with c1:
                uploaded = st.file_uploader(
                    "ZIP or TXT",
                    type=["zip", "txt", "TXT", "ZIP"],
                    key=f"file_{idx}"
                )
                st.session_state.rows[idx]["file"] = uploaded
            with c2:
                st.session_state.rows[idx]["county"] = st.selectbox(
                    "County", all_counties, index=0 if all_counties else None, key=f"county_{idx}"
                ) if all_counties else ""
            with c3:
                st.session_state.rows[idx]["layout"] = st.selectbox(
                    "Layout", ["All"] + list(DUMP_LAYOUTS.keys()), key=f"layout_{idx}"
                )

# ---------- RIGHT: Summary + Logs ----------
with right:
    st.subheader("Selection Summary")
    summary = []
    for i, r in enumerate(st.session_state.rows):
        summary.append({
            "Row": i+1,
            "File": r["file"].name if r["file"] else "(none)",
            "County": r["county"],
            "Layout": r["layout"],
        })
    st.dataframe(pd.DataFrame(summary), use_container_width=True, height=240)
    render_logs()

st.markdown("---")

# ---------- Run Button & Progress ----------
run_btn_col, _ = st.columns([1, 3])
with run_btn_col:
    start = st.button("▶️ Start Loading & Consolidation", type="primary", use_container_width=True)

if start:
    st.session_state.logs.clear()
    st.session_state.tables_by_layout = {}
    progress = st.progress(0)
    total_steps = sum(1 if r["layout"] != "All" else len(DUMP_LAYOUTS) for r in st.session_state.rows) + 1
    step = 0

    # Process each row
    for i, r in enumerate(st.session_state.rows, start=1):
        file = r["file"]
        county = r["county"]
        layout_choice = r["layout"]
        tax_year = st.session_state.tax_year

        if not file or not county or not layout_choice:
            log_to_ui(f"Row {i}: Missing inputs. Skipping.")
            step += 1
            progress.progress(min(step/total_steps, 1.0))
            continue

        # Persist upload to a temp file path
        try:
            suffix = ".zip" if file.name.lower().endswith(".zip") else ".txt"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(file.getbuffer())
                tmp_path = tmp.name
            log_to_ui(f"Row {i}: Uploaded file saved to temp: {tmp_path}")
        except Exception as e:
            log_to_ui(f"Row {i}: Could not save uploaded file ({e}). Skipping.")
            step += 1
            progress.progress(min(step/total_steps, 1.0))
            continue

        # Run per layout(s)
        def run_one_layout(layout_label):
            tbl = worker_full_flow(tmp_path, layout_label, county, tax_year)
            if tbl:
                st.session_state.tables_by_layout.setdefault(layout_label, []).append(tbl)

        if layout_choice == "All":
            for lt in DUMP_LAYOUTS.keys():
                log_to_ui(f"=== Row {i}: Starting workflow for '{lt}' ===")
                run_one_layout(lt)
                log_to_ui(f"=== Row {i}: Completed workflow for '{lt}' ===")
                step += 1
                progress.progress(min(step/total_steps, 1.0))
        else:
            log_to_ui(f"=== Row {i}: Starting workflow for '{layout_choice}' ===")
            run_one_layout(layout_choice)
            log_to_ui(f"=== Row {i}: Completed workflow for '{layout_choice}' ===")
            step += 1
            progress.progress(min(step/total_steps, 1.0))

        # Clean temp file
        try:
            os.unlink(tmp_path)
        except:
            pass

    # Consolidate per layout
    log_to_ui("Starting consolidation for all layouts...")
    consolidate_per_layout(st.session_state.tables_by_layout)
    step += 1
    progress.progress(1.0)

    # Final summary table
    st.success("Process completed. You can use FINAL tables for consolidation.")
    with right:
        final_rows = []
        username = getpass.getuser().replace(" ", "_")
        for layout in st.session_state.tables_by_layout.keys():
            final_tbl = f"{DUMP_LAYOUTS[layout]['base_table_name']}_FINAL_{username}"
            final_rows.append({"Layout": layout, "Final Table": final_tbl})
        if final_rows:
            st.subheader("Final Consolidated Tables")
            st.dataframe(pd.DataFrame(final_rows), use_container_width=True, height=200)

    render_logs()
