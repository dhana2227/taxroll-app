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
import glob
from pathlib import Path

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

# Configure Streamlit for large files
st.set_page_config(
    page_title="Taxroll Consolidation Manager - 10GB Support",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------- Layouts ----------------
DUMP_LAYOUTS = {
    "Appraisal Info": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\Appraisal Export Layout - 8.0.30 2.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{4,6}_APPRAISAL_INFO\.TXT",
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
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{4,6}_APPRAISAL_IMPROVEMENT_INFO.TXT(?:\.TXT)?",
        "int_columns": {'prop_id', 'imprv_id', 'imprv_homesite_pct', 'imprv_val'},
        "base_table_name": "APPRAISAL_IMPROVEMENT_INFO"
    },
    "Appraisal Improvement Detail": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_IMPRV_DETAIL.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{4,6}_APPRAISAL_IMPROVEMENT_DETAIL\.TXT",
        "int_columns": {'prop_id', 'imprv_id', 'imprv_det_id', 'detail_val', 'imprv_det_area', 'imprv_det_val'},
        "base_table_name": "APPRAISAL_IMPROVEMENT_DETAIL"
    },
    "Appraisal Land Detail": {
        "layout_path": r"C:\Users\dhanasekaranr\OneDrive - O`Connor and Associates\Taxroll\Python-AppInfo\LAYOUT_APPRAISAL_LAND_DETAIL.xlsx",
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{4,6}_APPRAISAL_LAND_DETAIL\.TXT",
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
        "txt_pattern": r"\d{4}-\d{2}-\d{2}_\d{4,6}_APPRAISAL_ABSTRACT_SUBDV\.TXT",
        "int_columns": set(),
        "numeric_columns": set(),
        "base_table_name": "APPRAISAL_ABSTRACT_SUBDV"
    }
}

# ---------------- Large File Processing Folder ----------------
LARGE_FILE_FOLDER = os.path.join(os.path.expanduser("~"), "TaxrollLargeFiles")


def ensure_large_file_folder():
    """Create the large file folder if it doesn't exist"""
    if not os.path.exists(LARGE_FILE_FOLDER):
        os.makedirs(LARGE_FILE_FOLDER)
        log_to_ui(f"Created large file folder: {LARGE_FILE_FOLDER}")
    return LARGE_FILE_FOLDER


# --------------- Streamlit helpers ---------------
def init_state():
    if "rows" not in st.session_state:
        st.session_state.rows = [{"file": None, "county": "", "layout": "All", "file_path": ""}]
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "tables_by_layout" not in st.session_state:
        st.session_state.tables_by_layout = {}  # layout -> [temp tables]
    if "tax_year" not in st.session_state:
        st.session_state.tax_year = "2026"


def log_to_ui(msg: str, module: str = "Main", final_table: str = None, status: str = None):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    username = getpass.getuser()
    entry = f"[{ts}] {msg}"

    # Append to session logs for UI
    st.session_state.logs.append(entry)
    if len(st.session_state.logs) > 1000:
        st.session_state.logs = st.session_state.logs[-1000:]

    # Save log into SQL


def render_logs():
    if st.session_state.get("logs"):
        log_text = "\n".join(st.session_state.logs[-50:])  # Show only last 50 entries
        st.text_area("Live Log (Last 50 entries)", value=log_text, height=220)


def format_file_size(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f}{size_names[i]}"


def get_available_files():
    """Get list of files available in the large file folder"""
    ensure_large_file_folder()
    files = []

    # Look for files with multiple extensions
    patterns = ['*.zip', '*.txt', '*.TXT', '*.ZIP']
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(LARGE_FILE_FOLDER, pattern)))

    log_to_ui(f"Found {len(files)} files in large file folder: {LARGE_FILE_FOLDER}")

    # Return just filenames with sizes
    file_info = []
    for file_path in files:
        filename = os.path.basename(file_path)
        try:
            size = os.path.getsize(file_path)
            file_info.append({
                'display': f"{filename} ({format_file_size(size)})",
                'path': file_path,
                'size': size,
                'filename': filename
            })
            log_to_ui(f"Available file: {filename} -> {file_path} ({format_file_size(size)})")
        except OSError as e:
            log_to_ui(f"Error getting size for {file_path}: {e}")
            continue

    return sorted(file_info, key=lambda x: x['display'])


# ---------------- DB helpers ----------------
def get_db_connection():
    return pyodbc.connect(CONN_STR)


def fetch_county_codes():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT DISTINCT {CAD_CODE_COL} FROM {CAD_TABLE} WHERE {CAD_STATE_COL}='TX' ORDER BY {CAD_CODE_COL}"
            )
            rows = [r[0] for r in cur.fetchall()]
        log_to_ui(f"Fetched {len(rows)} counties from DB")
        return rows
    except Exception as e:
        log_to_ui(f"Error fetching counties: {e}")
        return []


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
        return float(s)
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


# ---------------- Optimized Large File Processing ----------------
def process_large_file_in_chunks(
        file_path,
        layout_path,
        txt_pattern,
        int_columns,
        base_table_name,
        county_code=None,
        tax_year=None,
        numeric_columns=None,
        chunk_size=100000  # Process 100k lines at a time
):
    """Process extremely large files in chunks to avoid memory issues"""
    if numeric_columns is None:
        numeric_columns = set()

    try:
        file_size = os.path.getsize(file_path)
        log_to_ui(f"Processing large file: {os.path.basename(file_path)} ({format_file_size(file_size)})")

        # Handle ZIP extraction
        if file_path.lower().endswith(".zip"):
            with zipfile.ZipFile(file_path, 'r') as z:
                txt_files = [f for f in z.namelist() if re.fullmatch(txt_pattern, os.path.basename(f))]
                if not txt_files:
                    log_to_ui("TXT file not found in ZIP.")
                    return False
                txt_filename = sorted(txt_files, reverse=True)[0]
                extract_dir = os.path.dirname(file_path)
                log_to_ui(f"Extracting {txt_filename}...")
                z.extract(txt_filename, path=extract_dir)
                txt_path = os.path.join(extract_dir, txt_filename)
        else:
            txt_path = file_path

        # Load Excel layout
        layout_df = pd.read_excel(layout_path)
        layout_df.columns = [c.strip().lower() for c in layout_df.columns]
        field_col = next(c for c in layout_df.columns if 'field' in c and 'name' in c)
        start_col = next(c for c in layout_df.columns if c.startswith('start'))
        end_col = next(c for c in layout_df.columns if c.startswith('end'))
        layout_positions = [(c, int(s) - 1, int(e)) for c, s, e in
                            zip(layout_df[field_col], layout_df[start_col], layout_df[end_col])]
        field_names = [c for c, _, _ in layout_positions]

        # Create table first
        username = getpass.getuser().replace(" ", "_")
        table_name = f"{base_table_name}_{username}"
        if county_code and tax_year:
            table_name = f"{base_table_name}_{county_code}_{username}_{tax_year}"

        log_to_ui(f"Creating database table: {table_name}")
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

        # Process file in chunks
        txt_size = os.path.getsize(txt_path)
        log_to_ui(f"Processing TXT file in chunks: {format_file_size(txt_size)}")

        total_rows_processed = 0
        start_time = time.time()

        # Setup progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Database connection for bulk inserts
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        placeholders = ", ".join("?" for _ in field_names)
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        with open(txt_path, "r", encoding="cp1252", errors="replace") as f:
            chunk_buffer = []
            line_count = 0
            bytes_processed = 0

            for line in f:
                line_count += 1
                bytes_processed += len(line.encode('cp1252', errors='replace'))

                # Parse line
                row = []
                for c, start, end in layout_positions:
                    val = line[start:end].strip()
                    if c in int_columns:
                        row.append(safe_int(val))
                    elif c in numeric_columns:
                        row.append(safe_numeric(val))
                    else:
                        row.append(val if val else None)

                chunk_buffer.append(row)

                # Process chunk when buffer is full
                if len(chunk_buffer) >= chunk_size:
                    cursor.executemany(insert_sql, chunk_buffer)
                    conn.commit()

                    total_rows_processed += len(chunk_buffer)
                    elapsed = time.time() - start_time
                    rate = total_rows_processed / elapsed if elapsed > 0 else 0

                    # Update progress
                    progress = min(bytes_processed / txt_size, 1.0)
                    progress_bar.progress(progress)
                    status_text.text(
                        f"Processed: {total_rows_processed:,} rows ({rate:.0f}/sec, {format_file_size(bytes_processed)})")

                    log_to_ui(f"Processed chunk: {total_rows_processed:,} total rows ({rate:.0f} rows/sec)")
                    chunk_buffer.clear()

            # Process remaining rows
            if chunk_buffer:
                cursor.executemany(insert_sql, chunk_buffer)
                conn.commit()
                total_rows_processed += len(chunk_buffer)

        cursor.close()
        conn.close()

        # Clear progress
        progress_bar.empty()
        status_text.empty()

        elapsed = time.time() - start_time
        log_to_ui(f"Large file processing completed: {total_rows_processed:,} rows in {elapsed:.1f} seconds")

        return True, table_name

    except Exception as e:
        log_to_ui(f"ERROR during large file processing: {e}")
        return False


# ---------------- Consolidation ----------------
def consolidate_per_layout(tables_by_layout: Dict[str, List[str]]):
    """Consolidate tables per layout into final tables with username suffix"""
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
            log_to_ui(f"Creating consolidated table: {final_table}")
            cur.execute(f"SELECT * INTO {final_table} FROM ({union_sql}) as U")
            log_to_ui(f"Consolidation for '{layout}' complete: {final_table}")

            # Drop temp tables
            for t in tbls:
                try:
                    cur.execute(f"IF OBJECT_ID('{t}', 'U') IS NOT NULL DROP TABLE {t}")
                    log_to_ui(f"Dropped intermediate table: {t}")
                except Exception as e:
                    log_to_ui(f"Warning: could not drop {t} ({e})")
        conn.commit()
    log_to_ui("All consolidations complete.")


# ---------------- Worker ----------------
def worker_full_flow(
        file_path: str,
        dump_type: str,
        county_code: str,
        tax_year: str
) -> Optional[str]:
    """Process a single layout load with CADID update"""
    try:
        log_to_ui(f"Starting workflow for '{dump_type}'")
        config = DUMP_LAYOUTS[dump_type]
        layout_path = config["layout_path"]
        txt_pattern = config["txt_pattern"]
        int_columns = config["int_columns"]
        numeric_columns = set(config.get("numeric_columns", set()))
        base_table_name = config["base_table_name"]

        # Use optimized large file processing
        res = process_large_file_in_chunks(
            file_path,
            layout_path,
            txt_pattern,
            int_columns,
            base_table_name,
            county_code,
            tax_year,
            numeric_columns=numeric_columns,
            chunk_size=50000  # Smaller chunks for very large files
        )

        if not res:
            log_to_ui("Load failed.")
            return None

        _, table_name = res

        # Add CADID
        cadid = get_cad_id_for_code(county_code)
        if cadid is None:
            log_to_ui(f"No CADID found for county '{county_code}'")
            return table_name

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
init_state()

st.title("Data Loading - Appraisal info ")
st.caption("Optimized ETL for extremely large files (up to 10GB+)")

# File management instructions
with st.expander("📁 Handle Large Files (10GB+)", expanded=True):
    folder_path = ensure_large_file_folder()
    st.markdown(...)  # Truncated for brevity

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("📁 Open Large File Folder", help="Opens the folder in Windows Explorer"):
            os.startfile(folder_path)
    with col2:
        if st.button("🔄 Refresh File List"):
            st.rerun()

    # Get available large files here, before the debug expander
    available_files = get_available_files()

    # Debug information
    with st.expander("🔍 Debug Information", expanded=False):
        st.write("**Large File Folder:**", folder_path)
        st.write("**Files Found:**")

        # remove duplicates by filename
        unique_files = {f['filename']: f for f in available_files}.values()  # dedupe here

        for file_info in unique_files:
            st.write(f"- {file_info['filename']} ({format_file_size(file_info['size'])})")


# The rest of the application code continues below...
# Tax year selection
st.selectbox("Tax Year", ["2026", "2025", "2024", "2023"], key="tax_year")

# File selection area
st.subheader("File Selection")
col1, col2 = st.columns([2, 1])

with col1:
    # This call is now redundant and can be removed
    # available_files = get_available_files()

    # Add file selection rows
    if st.button("➕ Add Row"):
        if len(st.session_state.rows) < 30:
            st.session_state.rows.append({"file": None, "county": "", "layout": "All", "file_path": ""})

    if st.button("➖ Remove Last"):
        if len(st.session_state.rows) > 1:
            st.session_state.rows.pop()

with col2:
    # Counties
    @st.cache_data(show_spinner=False)
    def _get_counties():
        return fetch_county_codes()


    all_counties = _get_counties()

# File selection interface
for idx, row in enumerate(st.session_state.rows):
    with st.container():
        st.markdown(f"**Row {idx + 1}**")
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])

        with c1:
            # Option 1: Small file upload (< 200MB)
            # small_file = st.file_uploader(
            #     "Small File (< 200MB)",
            #     type=["zip", "txt", "TXT", "ZIP"],
            #     key=f"small_file_{idx}",
            #     help="For files under 200MB"
            # )

            # Option 2: Large file selection
            file_options = ["Select a large file..."] + [f['display'] for f in unique_files]

            selected_file = st.selectbox(
                "Select large file from folder:",
                file_options,
                key=f"large_file_{idx}"
            )

            # Update row data
            if selected_file != "Select a large file...":
                for file_info in available_files:
                    if file_info['display'] == selected_file:
                        st.session_state.rows[idx]["file"] = None
                        st.session_state.rows[idx]["file_path"] = file_info['path']
                        log_to_ui(f"Row {idx + 1}: Selected large file: {file_info['path']}")
                        break
            else:
                st.session_state.rows[idx]["file"] = None
                st.session_state.rows[idx]["file_path"] = ""

        with c2:
            st.session_state.rows[idx]["county"] = st.selectbox(
                "County", all_counties, index=0 if all_counties else None, key=f"county_{idx}"
            ) if all_counties else ""

        with c3:
            st.session_state.rows[idx]["layout"] = st.selectbox(
                "Layout", ["All"] + list(DUMP_LAYOUTS.keys()), key=f"layout_{idx}"
            )

        with c4:
            # Show file info
            if st.session_state.rows[idx]["file"]:
                file_size = st.session_state.rows[idx]["file"].size if hasattr(st.session_state.rows[idx]["file"],
                                                                               'size') else 0
                st.caption(f"📁 {format_file_size(file_size)}")
            elif st.session_state.rows[idx]["file_path"]:
                try:
                    file_size = os.path.getsize(st.session_state.rows[idx]["file_path"])
                    st.caption(f"💾 {format_file_size(file_size)}")
                except OSError:
                    st.caption("File not found")

# Processing section
left_col, right_col = st.columns([1.3, 1])

with left_col:
    if st.button("▶️ Start Processing", type="primary", use_container_width=True):
        st.session_state.logs.clear()
        st.session_state.tables_by_layout = {}

        # Process each row
        total_steps = sum(1 if r["layout"] != "All" else len(DUMP_LAYOUTS) for r in st.session_state.rows) + 1
        step = 0

        main_progress = st.progress(0)
        main_status = st.empty()

        for i, row in enumerate(st.session_state.rows, start=1):
            county = row["county"]
            layout_choice = row["layout"]
            tax_year = st.session_state.tax_year

            log_to_ui(f"Row {i}: Checking inputs - County: {county}, Layout: {layout_choice}")
            log_to_ui(f"Row {i}: File: {row.get('file', 'None')}, File Path: {row.get('file_path', 'None')}")

            # Determine file path
            file_path = None
            if row["file_path"] and os.path.exists(row["file_path"]):
                file_path = row["file_path"]
                log_to_ui(f"Row {i}: Using large file: {file_path}")
            else:
                log_to_ui(f"Row {i}: File path '{row.get('file_path', 'None')}' does not exist or is empty")


            def run_one_layout(layout_label):
                main_status.text(f"Row {i}: Processing layout '{layout_label}'...")
                tbl = worker_full_flow(file_path, layout_label, county, tax_year)
                if tbl:
                    st.session_state.tables_by_layout.setdefault(layout_label, []).append(tbl)


            if layout_choice == "All":
                for lt in DUMP_LAYOUTS.keys():
                    log_to_ui(f"=== Row {i}: Starting workflow for '{lt}' ===")
                    run_one_layout(lt)
                    step += 1
                    main_progress.progress(min(step / total_steps, 1.0))
            else:
                log_to_ui(f"=== Row {i}: Starting workflow for '{layout_choice}' ===")
                run_one_layout(layout_choice)
                step += 1
                main_progress.progress(min(step / total_steps, 1.0))

            # Cleanup temp file if created
            if row["file"] and file_path and file_path.startswith(tempfile.gettempdir()):
                try:
                    os.unlink(file_path)
                except:
                    pass

        # Consolidation
        main_status.text("Starting consolidation...")
        consolidate_per_layout(st.session_state.tables_by_layout)
        step += 1
        main_progress.progress(1.0)
        main_status.text("Processing completed!")

        # Clear progress indicators
        main_progress.empty()
        main_status.empty()

        st.success("✅ Processing completed successfully!")

with right_col:
    st.subheader("Summary & Logs")

    # Show summary
    summary_data = []
    for i, row in enumerate(st.session_state.rows):
        file_info = "None"
        if row["file"]:
            file_size = row["file"].size if hasattr(row["file"], 'size') else 0
            file_info = f"📁 {row['file'].name} ({format_file_size(file_size)})"
        elif row["file_path"]:
            file_size = os.path.getsize(row["file_path"]) if os.path.exists(row["file_path"]) else 0
            file_info = f"💾 {os.path.basename(row['file_path'])} ({format_file_size(file_size)})"

        summary_data.append({
            "Row": i + 1,
            "File": file_info,
            "County": row["county"],
            "Layout": row["layout"]
        })

    if summary_data:
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, height=200)

    # Show final tables if available
    if st.session_state.tables_by_layout:
        st.subheader("Final Tables Created")
        username = getpass.getuser().replace(" ", "_")
        final_tables = []
        for layout in st.session_state.tables_by_layout.keys():
            table_name = f"{DUMP_LAYOUTS[layout]['base_table_name']}_FINAL_{username}"
            final_tables.append({
                "Layout": layout,
                "Final Table": table_name
            })

        if final_tables:
            st.dataframe(pd.DataFrame(final_tables), use_container_width=True)

# Logs section
st.subheader("Processing Logs")
render_logs()
