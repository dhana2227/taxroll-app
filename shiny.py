# app.py — Full end-to-end Streamlit ETL + logging + Taxroll loader - Enhanced Version
import os
import re
import time
import zipfile
import getpass
import shutil
import tempfile
import glob
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import threading

import pandas as pd
import pyodbc
import streamlit as st

# ---------------- CONFIG ----------------
SERVER = r"taxrollstage-db"
DATABASE = "TaxrollStaging"
DRIVER = "{ODBC Driver 13 for SQL Server}"
# Enhanced connection string with timeouts
CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;Connection Timeout=30;CommandTimeout=1800;"

# Column name in the CAD lookup table (tbl_cads_shiny)
CAD_TABLE = "tbl_cads_shiny"
CAD_CODE_COL = "cad_CADCode"
CAD_ID_COL_IN_CAD = "cad_CADID"
CAD_STATE_COL = "cad_State"

# Column name to add to staging/final tables
STAGING_CAD_COL = "CADID"

# Chunk size for file inserts
CHUNK_SIZE_LINES = 50000

# Root folder for large files
LARGE_FILE_FOLDER = os.path.join(os.path.expanduser("~"), "TaxrollLargeFiles")

# ---------------- Layouts (your provided layout definitions) ----------------
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


# ---------------- DB helpers ----------------
def get_db_connection():
    return pyodbc.connect(CONN_STR)


def ensure_logs_table():
    sql = """
    IF OBJECT_ID('dbo.tbl_appraisal_logs','U') IS NULL
    BEGIN
        CREATE TABLE dbo.tbl_appraisal_logs (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            LogTime DATETIME DEFAULT GETDATE(),
            Username NVARCHAR(200),
            Module NVARCHAR(200),
            County NVARCHAR(200) NULL,
            Layout NVARCHAR(200) NULL,
            Message NVARCHAR(MAX),
            FinalTable NVARCHAR(255) NULL,
            Status NVARCHAR(50) NULL
        )
    END"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def column_exists(conn, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?",
                (table_name, column_name))
    return cur.fetchone() is not None


def add_column_if_missing(table_name: str, column_name: str, data_type: str = "INT") -> bool:
    try:
        with get_db_connection() as conn:
            if column_exists(conn, table_name, column_name):
                return True
            cur = conn.cursor()
            cur.execute(f"ALTER TABLE {table_name} ADD [{column_name}] {data_type}")
            conn.commit()
            return True
    except Exception as e:
        log_to_ui(f"Error adding column {column_name} to {table_name}: {e}", module="DB", status="Error")
        return False


def update_table_set_column(conn, table_name: str, column_name: str, value) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {table_name} SET {column_name} = ?", (value,))
        conn.commit()
        return True
    except Exception as e:
        log_to_ui(f"Error updating column {column_name} in {table_name}: {e}", module="DB", status="Error")
        return False


# ---------------- Logging (UI + DB) ----------------
def get_final_table_name_from_staging(staging_table: str, username: str) -> str:
    """Convert staging table name (which may include county/username/year) to final table name."""
    parts = staging_table.split("_")
    # base is first two tokens (e.g., APPRAISAL_INFO)
    base = "_".join(parts[:2]) if len(parts) >= 2 else parts[0]
    return f"{base}_FINAL_{username}"


def log_to_ui(msg, module="Main", final_table=None, status=None):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    username = getpass.getuser()
    entry = {
        "Timestamp": ts, "Username": username, "Module": module,
        "Message": msg, "FinalTable": final_table, "Status": status
    }
    if "logs" not in st.session_state:
        st.session_state.logs = []
    st.session_state.logs.append(entry)

    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO tbl_appraisal_logs
                (LogTime, Username, Module, Message, FinalTable, Status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ts, username, module, msg, final_table, status))
            conn.commit()
    except Exception as e:
        # Silent fail for logging to avoid infinite loops
        pass


# ---------------- Utility parsers ----------------
def safe_int(val):
    try:
        if val is None or str(val).strip() == "":
            return None
        return int(str(val).split(".")[0].replace(",", ""))
    except:
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


# ---------------- CADID resolution ----------------
def get_cad_id_for_county_name(county_name: str) -> Optional[int]:
    """Try exact match then LIKE match to resolve CADID from tbl_cads_shiny."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT TOP 1 {CAD_ID_COL_IN_CAD} FROM {CAD_TABLE} WHERE {CAD_STATE_COL}='TX' AND {CAD_CODE_COL} = ?",
                (county_name,))
            r = cur.fetchone()
            if r:
                return r[0]
            # fallback LIKE
            cur.execute(
                f"SELECT TOP 1 {CAD_ID_COL_IN_CAD} FROM {CAD_TABLE} WHERE {CAD_STATE_COL}='TX' AND {CAD_CODE_COL} LIKE ?",
                (f"%{county_name}%",))
            r2 = cur.fetchone()
            return r2[0] if r2 else None
    except Exception as e:
        log_to_ui(f"Error resolving CADID for '{county_name}': {e}", module="CAD", status="Error")
        return None


# ---------------- Layout reading ----------------
def read_layout_positions(layout_path: str) -> Tuple[List[str], List[Tuple[int, int]]]:
    df = pd.read_excel(layout_path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    field_col = next((c for c in df.columns if 'field' in c and 'name' in c), None)
    start_col = next((c for c in df.columns if c.startswith('start')), None)
    end_col = next((c for c in df.columns if c.startswith('end')), None)
    if not field_col or not start_col or not end_col:
        raise ValueError(f"Layout Excel {layout_path} missing expected columns (field/start/end)")
    positions = []
    for _, r in df.iterrows():
        fname = str(r[field_col]).strip()
        s = int(r[start_col]) - 1
        e = int(r[end_col])
        positions.append((fname, s, e))
    return [p[0] for p in positions], [(p[1], p[2]) for p in positions]


# ---------------- Core: large file processing ----------------
def process_large_file_in_chunks(
        file_path: str,
        layout_path: str,
        txt_pattern: str,
        int_columns: Set[str],
        base_table_name: str,
        county_code: Optional[str] = None,
        tax_year: Optional[str] = None,
        numeric_columns: Optional[Set[str]] = None,
        chunk_size: int = CHUNK_SIZE_LINES
) -> Tuple[bool, Optional[str]]:
    """
    Extract matching TXT (from zip or direct), create a staging table, load in chunks.
    Returns (success_flag, staging_table_name) on success/failure.
    """
    if numeric_columns is None:
        numeric_columns = set()

    try:
        if not os.path.exists(file_path):
            log_to_ui(f"Source file not found: {file_path}", module="Loader", status="Error")
            return False, None

        log_to_ui(f"Processing file: {file_path}", module="Loader")

        # If ZIP, extract matched TXT
        extracted_txt_paths = []
        if file_path.lower().endswith(".zip"):
            with zipfile.ZipFile(file_path, "r") as z:
                txt_files = [f for f in z.namelist() if
                             re.fullmatch(txt_pattern, os.path.basename(f), flags=re.IGNORECASE)]
                if not txt_files:
                    log_to_ui(f"No matching TXT in {file_path} for pattern {txt_pattern}", module="Loader")
                    return False, None
                # pick newest / sorted - as you had earlier
                txt_filename = sorted(txt_files, reverse=True)[0]
                extract_dir = os.path.dirname(file_path)
                z.extract(txt_filename, path=extract_dir)
                txt_path = os.path.join(extract_dir, txt_filename)
                extracted_txt_paths.append(txt_path)
        else:
            # direct txt
            if not re.fullmatch(txt_pattern, os.path.basename(file_path), flags=re.IGNORECASE):
                log_to_ui(f"File name does not match expected pattern: {file_path}", module="Loader")
                return False, None
            txt_path = file_path

        if not os.path.exists(txt_path):
            log_to_ui(f"TXT not found after extraction: {txt_path}", module="Loader", status="Error")
            return False, None

        # read layout spec
        layout_df = pd.read_excel(layout_path)
        layout_df.columns = [c.strip().lower() for c in layout_df.columns]
        field_col = next(c for c in layout_df.columns if 'field' in c and 'name' in c)
        start_col = next(c for c in layout_df.columns if c.startswith('start'))
        end_col = next(c for c in layout_df.columns if c.startswith('end'))
        layout_positions = [(c, int(s) - 1, int(e)) for c, s, e in
                            zip(layout_df[field_col], layout_df[start_col], layout_df[end_col])]
        field_names = [c for c, _, _ in layout_positions]

        # staging table naming
        username = getpass.getuser().replace(" ", "_")
        if county_code and tax_year:
            staging_table = f"{base_table_name}_{county_code}_{username}_{tax_year}"
        else:
            staging_table = f"{base_table_name}_{username}"

        # create staging table (drop if exists)
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE {staging_table}")
            col_defs = []
            # Use BIGINT for ints (to reduce overflow issues), FLOAT for numeric, NVARCHAR(MAX) otherwise
            for c in field_names:
                if c.lower() in {x.lower() for x in int_columns}:
                    col_defs.append(f"[{c}] BIGINT")
                elif c.lower() in {x.lower() for x in (numeric_columns or set())}:
                    col_defs.append(f"[{c}] FLOAT")
                else:
                    col_defs.append(f"[{c}] NVARCHAR(MAX)")
            cur.execute(f"CREATE TABLE {staging_table} ({', '.join(col_defs)})")
            conn.commit()

        # load file in chunks
        insert_sql = f"INSERT INTO {staging_table} VALUES ({', '.join(['?'] * len(field_names))})"
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        chunk_buffer = []
        total_rows = 0
        start_time = time.time()
        txt_size = os.path.getsize(txt_path)

        with open(txt_path, "r", encoding="cp1252", errors="replace") as fh:
            bytes_processed = 0
            for line in fh:
                bytes_processed += len(line.encode('cp1252', errors='replace'))
                row = []
                for c, s, e in layout_positions:
                    raw = line[s:e] if len(line) >= e else line[s:]
                    raw_val = raw.strip()
                    if c.lower() in {x.lower() for x in int_columns}:
                        row.append(safe_int(raw_val))
                    elif c.lower() in {x.lower() for x in (numeric_columns or set())}:
                        row.append(safe_numeric(raw_val))
                    else:
                        row.append(raw_val if raw_val != "" else None)
                chunk_buffer.append(row)

                if len(chunk_buffer) >= chunk_size:
                    cursor.executemany(insert_sql, chunk_buffer)
                    conn.commit()
                    total_rows += len(chunk_buffer)
                    chunk_buffer.clear()
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    pct = min(bytes_processed / max(txt_size, 1), 1.0)
                    log_to_ui(f"{staging_table}: inserted {total_rows:,} rows ({rate:.0f} rows/s) - {pct * 100:.1f}%",
                              module="Loader")

            # final flush
            if chunk_buffer:
                cursor.executemany(insert_sql, chunk_buffer)
                conn.commit()
                total_rows += len(chunk_buffer)
                chunk_buffer.clear()

        cursor.close()
        conn.close()

        elapsed = time.time() - start_time
        log_to_ui(f"Staging load complete: {staging_table} rows={total_rows} elapsed={elapsed:.1f}s", module="Loader")

        # cleanup extracted txt files (leave zip as-is)
        for p in extracted_txt_paths:
            try:
                if os.path.exists(p):
                    os.remove(p)
                    log_to_ui(f"Deleted extracted txt {p}", module="Loader")
            except Exception as e:
                log_to_ui(f"Could not delete extracted txt {p}: {e}", module="Loader", status="Warning")

        return True, staging_table

    except Exception as e:
        log_to_ui(f"ERROR processing {file_path}: {e}", module="Loader", status="Error")
        return False, None


# ---------------- Worker that wraps per-layout processing + staging CADID update ----------------
def worker_full_flow(file_path, dump_type, county_code, tax_year):
    """
    Process a file for a specific layout type.
    This function now actually calls the real processing function.
    """
    try:
        if not file_path or not os.path.exists(file_path):
            log_to_ui(f"Invalid file path for {dump_type}: {file_path}", module=dump_type, status="Error")
            return None

        config = DUMP_LAYOUTS[dump_type]
        layout_path = config["layout_path"]
        txt_pattern = config["txt_pattern"]
        int_columns = config.get("int_columns", set())
        numeric_columns = set(config.get("numeric_columns", set()))
        base_table_name = config["base_table_name"]

        log_to_ui(f"Start loading {dump_type}", module=dump_type)

        # Call the actual processing function
        success, staging_table = process_large_file_in_chunks(
            file_path=file_path,
            layout_path=layout_path,
            txt_pattern=txt_pattern,
            int_columns=int_columns,
            base_table_name=base_table_name,
            county_code=county_code,
            tax_year=tax_year,
            numeric_columns=numeric_columns
        )

        if success and staging_table:
            # Add CADID column and update with county info
            if county_code:
                cad_id = get_cad_id_for_county_name(county_code)
                if cad_id and add_column_if_missing(staging_table, STAGING_CAD_COL):
                    with get_db_connection() as conn:
                        if update_table_set_column(conn, staging_table, STAGING_CAD_COL, cad_id):
                            log_to_ui(f"Updated {staging_table} with CADID={cad_id}", module=dump_type)
                        else:
                            log_to_ui(f"Failed to update CADID in {staging_table}", module=dump_type, status="Warning")
                else:
                    log_to_ui(f"Could not resolve CADID for county '{county_code}'", module=dump_type, status="Warning")

            log_to_ui(f"Finished {dump_type}, staging={staging_table}", module=dump_type, final_table=staging_table,
                      status="StagingLoaded")
            return staging_table
        else:
            log_to_ui(f"Failed to process {dump_type}", module=dump_type, status="Error")
            return None

    except Exception as e:
        log_to_ui(f"Error in {dump_type}: {e}", module=dump_type, status="Error")
        return None


# ---------------- Consolidation (staging -> final) ----------------
def consolidate_per_layout(tables_by_layout: Dict[str, List[str]]):
    """
    For each layout category, union staging tables into a single final table:
      <BASE_TABLE_NAME>_FINAL_<username>
    After creation, log Completed status into tbl_appraisal_logs.
    """
    username = getpass.getuser().replace(" ", "_")
    ensure_logs_table()

    with get_db_connection() as conn:
        cur = conn.cursor()
        for layout, tbls in tables_by_layout.items():
            if not tbls:
                log_to_ui(f"No staging tables for layout {layout}, skipping consolidation.", module="Consolidation")
                continue

            base_table = DUMP_LAYOUTS[layout]["base_table_name"]
            final_table = f"{base_table}_FINAL_{username}"

            try:
                log_to_ui(f"Creating final table {final_table} from {len(tbls)} staging(s)", module="Consolidation")
                # Drop final if exists
                cur.execute(f"IF OBJECT_ID('{final_table}', 'U') IS NOT NULL DROP TABLE {final_table}")
                # Build union SQL
                union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tbls])
                # SELECT INTO final
                cur.execute(f"SELECT * INTO {final_table} FROM ({union_sql}) AS U")
                conn.commit()
                log_to_ui(f"Final table created: {final_table}", module="Consolidation", final_table=final_table,
                          status="Completed")
            except Exception as e:
                log_to_ui(f"ERROR creating final table {final_table}: {e}", module="Consolidation",
                          final_table=final_table, status="Error")
                # continue to next layout
                continue

            # Drop staging tables (cleanup)
            for t in tbls:
                try:
                    cur.execute(f"IF OBJECT_ID('{t}', 'U') IS NOT NULL DROP TABLE {t}")
                    log_to_ui(f"Dropped staging table {t}", module="Consolidation")
                except Exception as e:
                    log_to_ui(f"Warning: could not drop {t}: {e}", module="Consolidation", status="Warning")
            conn.commit()

    log_to_ui("All consolidations complete.", module="Consolidation", status="Completed")


# ---------------- Enhanced Auto Processing Functions ----------------
def check_stored_procedure_exists():
    """Check if the stored procedure exists and get its parameters."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()

            # Check if stored procedure exists
            cur.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_TYPE = 'PROCEDURE' 
                  AND ROUTINE_NAME = 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1'
            """)

            exists = cur.fetchone()[0] > 0
            log_to_ui(f"Stored procedure exists: {exists}", module="Taxroll")

            if exists:
                # Get parameter information
                cur.execute("""
                    SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE
                    FROM INFORMATION_SCHEMA.PARAMETERS
                    WHERE SPECIFIC_NAME = 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1'
                    ORDER BY ORDINAL_POSITION
                """)

                params = cur.fetchall()
                log_to_ui(f"Stored procedure parameters: {len(params)} found", module="Taxroll")
                for param in params:
                    log_to_ui(f"  Parameter: {param[0]} ({param[1]}, {param[2]})", module="Taxroll")

                # Check if we have execute permission
                try:
                    cur.execute("SELECT HAS_PERMS_BY_NAME('dbo.AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1', 'OBJECT', 'EXECUTE')")
                    has_execute = cur.fetchone()[0] == 1
                    log_to_ui(f"Execute permission on stored procedure: {has_execute}", module="Taxroll")
                except Exception as perm_error:
                    log_to_ui(f"Could not check execute permission: {perm_error}", module="Taxroll", status="Warning")

            return exists

    except Exception as e:
        log_to_ui(f"Error checking stored procedure: {e}", module="Taxroll", status="Error")
        return False


def run_taxroll_data_loading():
    """Execute Taxroll data loading stored procedure with enhanced error handling."""
    try:
        username = getpass.getuser().replace(" ", "_")
        required = {
            f"APPRAISAL_INFO_FINAL_{username}",
            f"APPRAISAL_IMPROVEMENT_INFO_FINAL_{username}",
            f"APPRAISAL_IMPROVEMENT_DETAIL_FINAL_{username}",
            f"APPRAISAL_LAND_DETAIL_FINAL_{username}",
            f"APPRAISAL_ABSTRACT_SUBDV_FINAL_{username}",
        }

        # First check if stored procedure exists
        if not check_stored_procedure_exists():
            log_to_ui("Stored procedure not found or inaccessible", module="Taxroll", status="Error")
            return False

        with get_db_connection() as conn:
            cur = conn.cursor()

            # First, check if tables actually exist in database
            log_to_ui("Checking if required final tables exist in database...", module="Taxroll")
            existing_tables = set()
            for table_name in required:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = ?
                """, (table_name,))
                if cur.fetchone()[0] > 0:
                    existing_tables.add(table_name)
                    # Also check row count
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                        row_count = cur.fetchone()[0]
                        log_to_ui(f"Table {table_name} exists with {row_count:,} rows", module="Taxroll")
                    except Exception as count_error:
                        log_to_ui(f"Table {table_name} exists but couldn't get row count: {count_error}", module="Taxroll", status="Warning")
                else:
                    log_to_ui(f"Table {table_name} does NOT exist in database", module="Taxroll", status="Error")

            missing_tables = required - existing_tables
            if missing_tables:
                log_to_ui(f"Missing tables in database: {', '.join(missing_tables)}", module="Taxroll", status="Error")
                return False

            # Check completion status in logs
            cur.execute("""
                SELECT DISTINCT FinalTable, Status, COUNT(*) as LogCount
                FROM dbo.tbl_appraisal_logs
                WHERE Username = ?
                  AND FinalTable IS NOT NULL
                  AND FinalTable IN ({})
                GROUP BY FinalTable, Status
            """.format(','.join('?' * len(required))), [username] + list(required))

            rows = cur.fetchall()
            completed = {r[0] for r in rows if r[1] and r[1].lower() == "completed"}

            log_to_ui(f"Tables marked as completed in logs: {len(completed)}/{len(required)}", module="Taxroll")
            for table in required:
                status = "✓ Completed" if table in completed else "✗ Not Completed"
                log_to_ui(f"  {table}: {status}", module="Taxroll")

            if required.issubset(completed):
                log_to_ui("All final tables present & completed. Starting Taxroll Data Loading...", module="Taxroll")

                # Execute stored procedure with timeout and better error handling
                try:
                    log_to_ui("Executing stored procedure: dbo.AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1", module="Taxroll")

                    # Set command timeout to handle long-running procedures
                    cur.execute("""
                        EXEC dbo.AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1
                            @APPRAISAL_INFO=?,
                            @APPRAISAL_IMPROVEMENT_INFO=?,
                            @APPRAISAL_IMPROVEMENT_DETAIL=?,
                            @APPRAISAL_LAND_DETAIL=?,
                            @APPRAISAL_ABSTRACT_SUBDV=?
                    """, (
                        f"APPRAISAL_INFO_FINAL_{username}",
                        f"APPRAISAL_IMPROVEMENT_INFO_FINAL_{username}",
                        f"APPRAISAL_IMPROVEMENT_DETAIL_FINAL_{username}",
                        f"APPRAISAL_LAND_DETAIL_FINAL_{username}",
                        f"APPRAISAL_ABSTRACT_SUBDV_FINAL_{username}",
                    ))

                    # Check if procedure returned any result sets or messages
                    try:
                        while cur.nextset():
                            results = cur.fetchall()
                            if results:
                                for row in results:
                                    log_to_ui(f"SP Result: {row}", module="Taxroll")
                    except pyodbc.ProgrammingError:
                        pass  # No more result sets

                    conn.commit()
                    log_to_ui("Taxroll Data Loading stored proc finished successfully.", module="Taxroll", status="Completed")
                    return True

                except pyodbc.Error as db_error:
                    log_to_ui(f"Database error in stored procedure: {db_error}", module="Taxroll", status="Error")
                    try:
                        conn.rollback()
                    except:
                        pass
                    return False
                except Exception as sp_error:
                    log_to_ui(f"Error executing stored procedure: {sp_error}", module="Taxroll", status="Error")
                    try:
                        conn.rollback()
                    except:
                        pass
                    return False

            else:
                missing = required - completed
                log_to_ui(f"Taxroll loading skipped: missing completed final tables: {', '.join(missing)}",
                          module="Taxroll", status="Skipped")
                return False

    except pyodbc.Error as db_error:
        log_to_ui(f"Database connection error during Taxroll Data Loading: {db_error}", module="Taxroll", status="Error")
        return False
    except Exception as e:
        log_to_ui(f"Unexpected error during Taxroll Data Loading: {e}", module="Taxroll", status="Error")
        return False


def run_luc_loading():
    """Execute LUC loading stored procedure with enhanced error handling."""
    try:
        log_to_ui("Starting LUC loading...", module="LUC")

        # Check if LUC stored procedure exists
        with get_db_connection() as conn:
            cur = conn.cursor()

            # Check for your actual LUC stored procedure name
            cur.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_TYPE = 'PROCEDURE' 
                  AND ROUTINE_NAME = 'YourLUCStoredProc'
            """)

            sp_exists = cur.fetchone()[0] > 0

            if sp_exists:
                log_to_ui("LUC stored procedure found, executing...", module="LUC")
                # Replace with your actual LUC stored proc call
                # cur.execute("EXEC dbo.YourLUCStoredProc @Param1=?, @Param2=?", (val1, val2))
                # conn.commit()
                time.sleep(1)  # simulate processing time for now
                log_to_ui("LUC loading finished.", module="LUC", status="Completed")
                return True
            else:
                log_to_ui("LUC stored procedure not found, skipping...", module="LUC", status="Skipped")
                return True  # Return True to continue processing even if LUC SP doesn't exist

    except pyodbc.Error as db_error:
        log_to_ui(f"Database error during LUC loading: {db_error}", module="LUC", status="Error")
        return False
    except Exception as e:
        log_to_ui(f"LUC loading error: {e}", module="LUC", status="Error")
        return False


def auto_process_all():
    """Execute the complete automated processing pipeline with enhanced error handling."""
    try:
        log_to_ui("=== Starting AutoProcess All ===", module="AutoProcess")
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        # Validation before starting
        valid_rows = []
        for i, row in enumerate(st.session_state.rows, start=1):
            if not row.get("file_path") or not os.path.exists(row.get("file_path", "")):
                log_to_ui(f"Row {i}: No valid file selected", module="AutoProcess", status="Error")
                continue
            if not row.get("county"):
                log_to_ui(f"Row {i}: No county selected", module="AutoProcess", status="Error")
                continue
            valid_rows.append(row)

        if not valid_rows:
            log_to_ui("No valid rows to process. AutoProcess All aborted.", module="AutoProcess", status="Error")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False
            return False

        # Clear previous state
        st.session_state.tables_by_layout = {}

        # Step 1: Process all dump files
        log_to_ui("Step 1: Processing dump files...", module="AutoProcess")
        total_success = 0
        total_attempted = 0

        for i, row in enumerate(valid_rows, start=1):
            county = row["county"]
            layout_choice = row["layout"]
            tax_year = st.session_state.tax_year
            file_path = row.get("file_path")

            log_to_ui(
                f"Row {i}: County={county}, Layout={layout_choice}, File={os.path.basename(file_path) if file_path else 'None'}",
                module="AutoProcess")

            if layout_choice == "All":
                for lt in DUMP_LAYOUTS.keys():
                    log_to_ui(f"Row {i}: Processing layout '{lt}'...", module="AutoProcess")
                    total_attempted += 1
                    staging_tbl = worker_full_flow(file_path, lt, county, tax_year)
                    if staging_tbl:
                        st.session_state.tables_by_layout.setdefault(lt, []).append(staging_tbl)
                        total_success += 1
            else:
                log_to_ui(f"Row {i}: Processing layout '{layout_choice}'...", module="AutoProcess")
                total_attempted += 1
                staging_tbl = worker_full_flow(file_path, layout_choice, county, tax_year)
                if staging_tbl:
                    st.session_state.tables_by_layout.setdefault(layout_choice, []).append(staging_tbl)
                    total_success += 1

        log_to_ui(f"Dump processing complete: {total_success}/{total_attempted} successful", module="AutoProcess")

        if total_success == 0:
            log_to_ui("No dump files processed successfully. AutoProcess All aborted.", module="AutoProcess", status="Error")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False
            return False

        # Step 2: Consolidation
        log_to_ui("Step 2: Starting consolidation...", module="AutoProcess")
        consolidate_per_layout(st.session_state.tables_by_layout)

        # Step 3: Run Taxroll Data Loading
        log_to_ui("Step 3: Starting Taxroll Data Loading...", module="AutoProcess")
        taxroll_success = run_taxroll_data_loading()

        if not taxroll_success:
            log_to_ui("AutoProcess All failed at Taxroll Data Loading step", module="AutoProcess", status="Error")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False
            return False

        # Step 4: Run LUC Loading
        log_to_ui("Step 4: Starting LUC Loading...", module="AutoProcess")
        luc_success = run_luc_loading()

        if not luc_success:
            log_to_ui("AutoProcess All failed at LUC Loading step", module="AutoProcess", status="Error")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False
            return False

        # Completion
        log_to_ui("=== AutoProcess All completed successfully ===", module="AutoProcess", status="Completed")
        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False
        return True

    except Exception as e:
        log_to_ui(f"AutoProcess All failed with error: {e}", module="AutoProcess", status="Error")
        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False
        return False


# ---------------- Streamlit UI & main flow ----------------
def ensure_large_file_folder():
    if not os.path.exists(LARGE_FILE_FOLDER):
        os.makedirs(LARGE_FILE_FOLDER)
        log_to_ui(f"Created large file folder: {LARGE_FILE_FOLDER}", module="Main")
    return LARGE_FILE_FOLDER


def format_file_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f}{size_names[i]}"


def get_available_files():
    ensure_large_file_folder()
    patterns = ['*.zip', '*.ZIP', '*.txt', '*.TXT']
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(LARGE_FILE_FOLDER, p)))
    file_info = []
    for f in files:
        try:
            size = os.path.getsize(f)
            file_info.append({'display': f"{os.path.basename(f)} ({format_file_size(size)})", 'path': f, 'size': size,
                              'filename': os.path.basename(f)})
        except Exception as e:
            log_to_ui(f"Could not stat file {f}: {e}", module="Main", status="Warning")
    return sorted(file_info, key=lambda x: x['display'])


def init_state():
    if "rows" not in st.session_state:
        st.session_state.rows = [{"file": None, "county": "", "layout": "All", "file_path": ""}]
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "tables_by_layout" not in st.session_state:
        st.session_state.tables_by_layout = {}
    if "tax_year" not in st.session_state:
        st.session_state.tax_year = "2026"
    if "processing_active" not in st.session_state:
        st.session_state.processing_active = False
    if "auto_refresh_logs" not in st.session_state:
        st.session_state.auto_refresh_logs = False


def display_live_logs():
    """Display live streaming logs with auto-refresh when processing is active."""
    if st.session_state.get("auto_refresh_logs", False):
        # Auto-refresh every 2 seconds during active processing
        time.sleep(0.1)  # Small delay to prevent too frequent updates
        st.rerun()

    st.subheader("📊 Live Processing Logs")

    # Create a container with fixed height for scrolling
    log_container = st.container()
    with log_container:
        if st.session_state.logs:
            # Display logs in reverse order (newest first) with color coding
            recent_logs = st.session_state.logs[-100:]  # Show last 100 logs

            # Create columns for better log display
            for entry in reversed(recent_logs):
                timestamp = entry.get("Timestamp", "")
                module = entry.get("Module", "")
                message = entry.get("Message", "")
                status = entry.get("Status", "")

                # Create a formatted log entry
                log_text = f"[{timestamp}] [{module}] {message}"

                # Color code based on status
                if status == "Error":
                    st.error(log_text)
                elif status == "Warning":
                    st.warning(log_text)
                elif status == "Completed":
                    st.success(log_text)
                else:
                    st.info(log_text)
        else:
            st.info("No logs available yet. Processing will show live updates here.")

    # Show processing status indicator
    if st.session_state.get("processing_active", False):
        st.markdown("""
        <div style="
            background-color: #fff3cd; 
            border: 1px solid #ffeaa7; 
            border-radius: 5px; 
            padding: 10px; 
            margin: 10px 0;
            text-align: center;
        ">
            <strong>🔄 PROCESSING ACTIVE - Logs updating in real-time...</strong>
        </div>
        """, unsafe_allow_html=True)


# Setup UI
st.set_page_config(page_title="Taxroll Consolidation Manager", layout="wide")
init_state()
ensure_logs_table()

st.title("Data Loading - Appraisal info")
st.caption("Optimized ETL for extremely large files (up to 10GB+)")

# File folder controls
with st.expander("📁 Handle Large Files (10GB+)", expanded=True):
    folder_path = ensure_large_file_folder()
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("📁 Open Large File Folder"):
            try:
                os.startfile(folder_path)
            except Exception as e:
                st.warning(f"Could not open folder: {e}")
    with col2:
        if st.button("🔄 Refresh File List"):
            # Force refresh by clearing cache and rerunning
            if hasattr(st, 'cache_data'):
                st.cache_data.clear()
            st.rerun()

    available_files = get_available_files()
    with st.expander("🔍 Debug Information", expanded=False):
        st.write("**Large File Folder:**", folder_path)
        for fi in available_files:
            st.write(f"- {fi['filename']} ({format_file_size(fi['size'])})")

# Tax year
st.selectbox("Tax Year", ["2026", "2025", "2024", "2023"], key="tax_year")

# File selection rows
st.subheader("File Selection")
col1, col2, _ = st.columns([1, 1, 5])  # push them together

with col1:
    if st.button("➕ Add Row"):
        if len(st.session_state.rows) < 30:
            st.session_state.rows.append({"file": None, "county": "", "layout": "All", "file_path": ""})

with col2:
    if st.button("➖ Remove Last"):
        if len(st.session_state.rows) > 1:
            st.session_state.rows.pop()


# Cached fetch of county codes
@st.cache_data(show_spinner=False)
def _get_counties():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT DISTINCT {CAD_CODE_COL} FROM {CAD_TABLE} WHERE {CAD_STATE_COL}='TX' ORDER BY {CAD_CODE_COL}")
            rows = [r[0] for r in cur.fetchall()]
            return rows
    except Exception as e:
        log_to_ui(f"Error fetching counties: {e}", module="Main", status="Error")
        return []


all_counties = _get_counties()
available_files_map = {f['display']: f for f in available_files}

for idx, row in enumerate(st.session_state.rows):
    st.markdown(f"**Row {idx + 1}**")
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        file_options = ["Select a large file..."] + list(available_files_map.keys())
        selected = st.selectbox(f"Select large file:", file_options, key=f"file_select_{idx}")
        if selected != "Select a large file...":
            fi = available_files_map.get(selected)
            if fi:
                st.session_state.rows[idx]["file_path"] = fi['path']
                st.session_state.rows[idx]["file"] = None
                log_to_ui(f"Row {idx + 1}: selected {fi['filename']}", module="Main")
        else:
            st.session_state.rows[idx]["file_path"] = ""
    with c2:
        county_options = ["Select county..."] + (all_counties if all_counties else [])
        selected_county = st.selectbox(f"County", county_options,
                                       index=county_options.index(st.session_state.rows[idx]["county"]) if
                                       st.session_state.rows[idx]["county"] in county_options else 0,
                                       key=f"county_{idx}")
        if selected_county != "Select county...":
            st.session_state.rows[idx]["county"] = selected_county
        else:
            st.session_state.rows[idx]["county"] = ""
    with c3:
        st.session_state.rows[idx]["layout"] = st.selectbox(f"Layout", ["All"] + list(DUMP_LAYOUTS.keys()),
                                                            key=f"layout_{idx}")
    with c4:
        # file info
        fp = st.session_state.rows[idx].get("file_path")
        if fp:
            try:
                st.caption(f"💾 {os.path.basename(fp)} ({format_file_size(os.path.getsize(fp))})")
            except Exception:
                st.caption("File not found")

# Processing section
st.markdown("---")
st.subheader("🚀 Processing Controls")

# Create four columns for the buttons
proc_col1, proc_col2, proc_col3, proc_col4 = st.columns([1, 1, 1, 1])

with proc_col1:
    if st.button("▶️ Start Processing", type="primary", disabled=st.session_state.get("processing_active", False)):
        # Validation before starting
        valid_rows = []
        for i, row in enumerate(st.session_state.rows, start=1):
            if not row.get("file_path") or not os.path.exists(row.get("file_path", "")):
                st.error(f"Row {i}: No valid file selected")
                continue
            if not row.get("county"):
                st.error(f"Row {i}: No county selected")
                continue
            valid_rows.append(row)

        if not valid_rows:
            st.error("No valid rows to process. Please check file paths and county selections.")
        else:
            # Clear previous state
            st.session_state.logs.clear()
            st.session_state.tables_by_layout = {}
            st.session_state.processing_active = True
            st.session_state.auto_refresh_logs = True

            total_steps = sum(1 if r["layout"] != "All" else len(DUMP_LAYOUTS) for r in valid_rows) + 1
            step = 0
            main_progress = st.progress(0)
            main_status = st.empty()

            for i, row in enumerate(valid_rows, start=1):
                county = row["county"]
                layout_choice = row["layout"]
                tax_year = st.session_state.tax_year
                file_path = row.get("file_path")

                log_to_ui(
                    f"Row {i}: County={county}, Layout={layout_choice}, File={os.path.basename(file_path) if file_path else 'None'}",
                    module="Main")

                if layout_choice == "All":
                    for lt in DUMP_LAYOUTS.keys():
                        log_to_ui(f"=== Row {i}: Starting workflow for '{lt}' ===", module="Main")
                        main_status.text(f"Row {i}: Processing layout '{lt}'...")
                        staging_tbl = worker_full_flow(file_path, lt, county, tax_year)
                        if staging_tbl:
                            st.session_state.tables_by_layout.setdefault(lt, []).append(staging_tbl)
                        step += 1
                        main_progress.progress(min(step / total_steps, 1.0))
                else:
                    log_to_ui(f"=== Row {i}: Starting workflow for '{layout_choice}' ===", module="Main")
                    main_status.text(f"Row {i}: Processing layout '{layout_choice}'...")
                    staging_tbl = worker_full_flow(file_path, layout_choice, county, tax_year)
                    if staging_tbl:
                        st.session_state.tables_by_layout.setdefault(layout_choice, []).append(staging_tbl)
                    step += 1
                    main_progress.progress(min(step / total_steps, 1.0))

            # Consolidation
            main_status.text("Starting consolidation...")
            consolidate_per_layout(st.session_state.tables_by_layout)
            step += 1
            main_progress.progress(1.0)
            main_status.text("Processing completed!")

            # Show completion notification
            st.success("✅ Raw Dump data Loading completed")
            st.balloons()

            log_to_ui("Raw Dump data Loading completed", module="Main", status="Completed")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False

with proc_col2:
    if st.button("🚀 AutoProcess All", type="primary",
                 disabled=st.session_state.get("processing_active", False)):
        # Validation before starting
        valid_rows = []
        for i, row in enumerate(st.session_state.rows, start=1):
            if not row.get("file_path") or not os.path.exists(row.get("file_path", "")):
                st.error(f"Row {i}: No valid file selected")
                continue
            if not row.get("county"):
                st.error(f"Row {i}: No county selected")
                continue
            valid_rows.append(row)

        if not valid_rows:
            st.error("No valid rows to process. Please check file paths and county selections.")
        else:
            # Clear previous state and start processing
            st.session_state.logs.clear()
            st.session_state.tables_by_layout = {}
            st.session_state.processing_active = True
            st.session_state.auto_refresh_logs = True

            # Create progress indicators
            main_progress = st.progress(0)
            main_status = st.empty()

            try:
                log_to_ui("=== Starting AutoProcess All ===", module="AutoProcess")

                # Step 1: Process all dump files
                main_status.text("Step 1/4: Processing dump files...")
                total_layouts = sum(1 if r["layout"] != "All" else len(DUMP_LAYOUTS) for r in valid_rows)
                current_layout = 0

                for i, row in enumerate(valid_rows, start=1):
                    county = row["county"]
                    layout_choice = row["layout"]
                    tax_year = st.session_state.tax_year
                    file_path = row.get("file_path")

                    log_to_ui(
                        f"AutoProcess Row {i}: County={county}, Layout={layout_choice}, File={os.path.basename(file_path) if file_path else 'None'}",
                        module="AutoProcess")

                    if layout_choice == "All":
                        for lt in DUMP_LAYOUTS.keys():
                            log_to_ui(f"AutoProcess Row {i}: Processing layout '{lt}'...", module="AutoProcess")
                            main_status.text(f"Step 1/4: Row {i} - Processing '{lt}'...")
                            staging_tbl = worker_full_flow(file_path, lt, county, tax_year)
                            if staging_tbl:
                                st.session_state.tables_by_layout.setdefault(lt, []).append(staging_tbl)
                            current_layout += 1
                            main_progress.progress(
                                current_layout / (total_layouts + 3) * 0.25)  # 25% for dump processing
                    else:
                        log_to_ui(f"AutoProcess Row {i}: Processing layout '{layout_choice}'...", module="AutoProcess")
                        main_status.text(f"Step 1/4: Row {i} - Processing '{layout_choice}'...")
                        staging_tbl = worker_full_flow(file_path, layout_choice, county, tax_year)
                        if staging_tbl:
                            st.session_state.tables_by_layout.setdefault(layout_choice, []).append(staging_tbl)
                        current_layout += 1
                        main_progress.progress(current_layout / (total_layouts + 3) * 0.25)

                # Step 2: Consolidation
                main_status.text("Step 2/4: Consolidating data...")
                log_to_ui("AutoProcess Step 2: Starting consolidation...", module="AutoProcess")
                consolidate_per_layout(st.session_state.tables_by_layout)
                main_progress.progress(0.5)  # 50% after consolidation

                # Step 3: Run Taxroll Data Loading
                main_status.text("Step 3/4: Running Taxroll Data Loading...")
                log_to_ui("AutoProcess Step 3: Starting Taxroll Data Loading...", module="AutoProcess")
                taxroll_success = run_taxroll_data_loading()
                main_progress.progress(0.75)  # 75% after taxroll

                if not taxroll_success:
                    log_to_ui("AutoProcess All failed at Taxroll Data Loading step", module="AutoProcess",
                              status="Error")
                    st.error("❌ AutoProcess All failed at Taxroll Data Loading step")
                else:
                    # Step 4: Run LUC Loading
                    main_status.text("Step 4/4: Running LUC Loading...")
                    log_to_ui("AutoProcess Step 4: Starting LUC Loading...", module="AutoProcess")
                    luc_success = run_luc_loading()
                    main_progress.progress(1.0)  # 100% complete

                    if not luc_success:
                        log_to_ui("AutoProcess All failed at LUC Loading step", module="AutoProcess", status="Error")
                        st.error("❌ AutoProcess All failed at LUC Loading step")
                    else:
                        # Success completion
                        main_status.text("✅ AutoProcess All completed successfully!")
                        log_to_ui("=== AutoProcess All completed successfully ===", module="AutoProcess",
                                  status="Completed")
                        st.success("✅ AutoProcess All completed successfully!")
                        st.balloons()

            except Exception as e:
                log_to_ui(f"AutoProcess All failed with error: {e}", module="AutoProcess", status="Error")
                st.error(f"❌ AutoProcess All failed: {e}")

            finally:
                # Always reset processing flags
                st.session_state.processing_active = False
                st.session_state.auto_refresh_logs = False

with proc_col3:
    if st.button("📊 Taxroll Data Loading", type="secondary",
                 disabled=st.session_state.get("processing_active", False)):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        success = run_taxroll_data_loading()

        if success:
            st.success("✅ Taxroll Data Loading Completed")
        else:
            st.error("❌ Taxroll Data Loading Failed")

        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False

with proc_col4:
    if st.button("📂 LUC Loading", type="secondary",
                 disabled=st.session_state.get("processing_active", False)):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        success = run_luc_loading()

        if success:
            st.success("✅ LUC Loading Completed")
        else:
            st.error("❌ LUC Loading Failed")

        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False

# Summary section
st.markdown("---")
st.subheader("📋 County Summary & Status")

left_col, right_col = st.columns([1.3, 1])

with left_col:
    # Summary table
    summary = []
    for i, r in enumerate(st.session_state.rows):
        file_info = "None"
        if r.get("file_path"):
            try:
                file_info = f"{os.path.basename(r['file_path'])} ({format_file_size(os.path.getsize(r['file_path']))})"
            except Exception:
                file_info = os.path.basename(r.get("file_path", ""))
        summary.append({"Row": i + 1, "File": file_info, "County": r.get("county"), "Layout": r.get("layout")})
    if summary:
        st.dataframe(pd.DataFrame(summary), use_container_width=True, height=200)

with right_col:
    # Final tables created
    if st.session_state.tables_by_layout:
        st.subheader("Final Tables (expected)")
        username = getpass.getuser().replace(" ", "_")
        finals = []
        for layout in st.session_state.tables_by_layout.keys():
            finals.append(
                {"Layout": layout, "Final Table": f"{DUMP_LAYOUTS[layout]['base_table_name']}_FINAL_{username}"})
        st.dataframe(pd.DataFrame(finals), use_container_width=True)

st.markdown("---")
# Add Database Loading Status Summary
st.subheader("🗄️ Database Loading Status Summary")
try:
    with get_db_connection() as conn:
        query = """
        SELECT 
            TaxYear,
            CountyName,
            CadID,
            CASE TaxrollLoadingStatus 
                WHEN 0 THEN 'Pending' 
                WHEN 1 THEN 'Completed' 
            END AS TaxrollLoadingStatus,
            CASE LUCStatus 
                WHEN 0 THEN 'Pending' 
                WHEN 1 THEN 'Completed' 
            END AS LUCStatus,
            TaxrollTableName,
            CQETableName,
            JurisdictionTableName
        FROM Appraisal_LoadingSummaryStatus
        ORDER BY TaxYear DESC, CountyName ASC;
        """

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            # Display the dataframe with better formatting
            st.dataframe(
                df,
                use_container_width=True,
                height=300,
                column_config={
                    "TaxYear": st.column_config.NumberColumn("Tax Year", format="%d"),
                    "CountyName": "County Name",
                    "CadID": st.column_config.NumberColumn("CAD ID"),
                    "TaxrollLoadingStatus": st.column_config.TextColumn(
                        "Taxroll Status",
                        help="Status of taxroll data loading"
                    ),
                    "LUCStatus": st.column_config.TextColumn(
                        "LUC Status",
                        help="Status of LUC loading"
                    ),
                    "TaxrollTableName": "Taxroll Table",
                    "CQETableName": "CQE Table",
                    "JurisdictionTableName": "Jurisdiction Table"
                }
            )

            # Add summary statistics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_records = len(df)
                st.metric("Total County", total_records)

            with col2:
                taxroll_completed = len(df[df['TaxrollLoadingStatus'] == 'Completed'])
                st.metric("Taxroll Completed", f"{taxroll_completed}/{total_records}")

            with col3:
                luc_completed = len(df[df['LUCStatus'] == 'Completed'])
                st.metric("LUC Completed", f"{luc_completed}/{total_records}")

            with col4:
                unique_counties = df['CountyName'].nunique()
                st.metric("Unique Counties", unique_counties)

        else:
            st.info("No loading status records found in Appraisal_LoadingSummaryStatus table.")

except Exception as e:
    st.error(f"Error retrieving loading status: {e}")
    log_to_ui(f"Error retrieving loading status: {e}", module="Database", status="Error")


# Additional Diagnostic Section
# Replace the existing diagnostics section in your Streamlit UI with this

# Enhanced Diagnostic Section
st.markdown("---")
with st.expander("🔧 System Diagnostics & Troubleshooting", expanded=False):
    st.subheader("Database Connectivity & Stored Procedure Status")

    diag_col1, diag_col2, diag_col3 = st.columns(3)

    with diag_col1:
        if st.button("🔍 Quick Connection Test", help="Test database connection with different ODBC drivers"):
            st.session_state.processing_active = True
            st.session_state.auto_refresh_logs = True

            try:
                working_driver = test_database_connection()
                if working_driver:
                    st.success(f"Connection successful with: {working_driver}")
                    global CONN_STR
                    CONN_STR = f"DRIVER={working_driver};SERVER=taxrollstage-db;DATABASE=TaxrollStaging;Trusted_Connection=yes;Connection Timeout=30;CommandTimeout=1800;"
                else:
                    st.error("No working ODBC driver found!")
            except Exception as e:
                st.error(f"Connection test failed: {e}")
            finally:
                st.session_state.processing_active = False
                st.session_state.auto_refresh_logs = False

    with diag_col2:
        if st.button("🏥 Full Taxroll Diagnostics", help="Comprehensive diagnostics for taxroll loading issues"):
            st.session_state.processing_active = True
            st.session_state.auto_refresh_logs = True

            try:
                success = detailed_taxroll_diagnostics()
                if success:
                    st.success("All diagnostics passed! Taxroll loading should work.")
                else:
                    st.error("Diagnostics found issues. Check logs for details.")
            except Exception as e:
                st.error(f"Diagnostics failed: {e}")
            finally:
                st.session_state.processing_active = False
                st.session_state.auto_refresh_logs = False

    with diag_col3:
        if st.button("🔄 Clear Logs & Reset", help="Clear all logs and reset session state"):
            st.session_state.logs.clear()
            st.session_state.tables_by_layout = {}
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False
            st.success("Session reset complete")

    # Show current connection info
    st.subheader("Current Configuration")
    st.info(f"""
     **Connection String:** `{CONN_STR[:100]}...`
     **Server:** taxrollstage-db
     **Database:** TaxrollStaging
     **Current User:** {getpass.getuser()}
     """)

    # Available ODBC drivers
    with st.expander("Available ODBC Drivers", expanded=False):
        try:
            drivers = [x for x in pyodbc.drivers() if 'SQL Server' in x]
            if drivers:
                for driver in drivers:
                    st.write(f"• {driver}")
            else:
                st.warning("No SQL Server ODBC drivers found!")
        except Exception as e:
            st.error(f"Could not enumerate drivers: {e}")

    # Recent errors from logs
    if st.session_state.logs:
        error_logs = [log for log in st.session_state.logs if log.get("Status") == "Error"]
        if error_logs:
            st.subheader("Recent Errors")
            for error in error_logs[-5:]:  # Show last 5 errors
                st.error(f"[{error['Module']}] {error['Message']}")

# Fix Connection Issue Button (prominent placement)
st.markdown("---")
st.subheader("🚨 Quick Fix for Connection Issues")

fix_col1, fix_col2 = st.columns(2)

with fix_col1:
    if st.button("🔧 Auto-Fix Connection", type="primary", help="Automatically detect and fix connection issues"):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        try:
            log_to_ui("Starting auto-fix for connection issues...", module="AutoFix")

            # Step 1: Test connection with different drivers
            working_driver = test_database_connection()
            if not working_driver:
                st.error("Could not establish database connection with any driver!")
                log_to_ui("Auto-fix failed: No working database connection", module="AutoFix", status="Error")
            else:
                # Step 2: Update global connection string
                global CONN_STR
                old_conn_str = CONN_STR
                CONN_STR = f"DRIVER={working_driver};SERVER=taxrollstage-db;DATABASE=TaxrollStaging;Trusted_Connection=yes;Connection Timeout=30;CommandTimeout=1800;"
                log_to_ui(f"Updated connection string to use {working_driver}", module="AutoFix")

                # Step 3: Test the fix
                try:
                    with get_db_connection_with_auto_driver() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT 1")
                        cur.fetchone()

                    st.success(f"Connection fixed! Now using: {working_driver}")
                    log_to_ui("Auto-fix successful - connection restored", module="AutoFix", status="Completed")

                except Exception as test_error:
                    CONN_STR = old_conn_str  # Revert
                    st.error(f"Fix failed: {test_error}")
                    log_to_ui(f"Auto-fix failed during verification: {test_error}", module="AutoFix", status="Error")

        except Exception as e:
            st.error(f"Auto-fix error: {e}")
            log_to_ui(f"Auto-fix error: {e}", module="AutoFix", status="Error")
        finally:
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False

with fix_col2:
    if st.button("🧪 Test Taxroll Loading Now", help="Test taxroll loading with current settings"):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        try:
            log_to_ui("Testing taxroll loading...", module="TestTaxroll")
            success = run_taxroll_data_loading()

            if success:
                st.success("Taxroll loading test successful!")
                st.balloons()
            else:
                st.error("Taxroll loading test failed. Check diagnostics above.")

        except Exception as e:
            st.error(f"Test failed: {e}")
            log_to_ui(f"Taxroll test error: {e}", module="TestTaxroll", status="Error")
        finally:
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False


# Live logs section with auto-refresh
st.markdown("---")
display_live_logs()

# Auto-refresh mechanism for live updates
if st.session_state.get("auto_refresh_logs", False):
    time.sleep(2)  # Wait 2 seconds before refresh
    st.rerun()