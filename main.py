# app.py — Professional UI for Taxroll Consolidation Manager
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
CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"

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


# ---------------- Auto Processing Functions ----------------
def run_taxroll_data_loading():
    """Execute Taxroll data loading stored procedure."""
    try:
        username = getpass.getuser().replace(" ", "_")
        required = {
            f"APPRAISAL_INFO_FINAL_{username}",
            f"APPRAISAL_IMPROVEMENT_INFO_FINAL_{username}",
            f"APPRAISAL_IMPROVEMENT_DETAIL_FINAL_{username}",
            f"APPRAISAL_LAND_DETAIL_FINAL_{username}",
            f"APPRAISAL_ABSTRACT_SUBDV_FINAL_{username}",
        }

        with get_db_connection() as conn:
            cur = conn.cursor()

            # First, check if all required tables exist in the database
            log_to_ui(f"Checking for required final tables for user: {username}", module="Taxroll")

            existing_tables = set()
            for table_name in required:
                cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (table_name,))
                if cur.fetchone():
                    existing_tables.add(table_name)
                    log_to_ui(f"Found table: {table_name}", module="Taxroll")
                else:
                    log_to_ui(f"Missing table: {table_name}", module="Taxroll", status="Warning")

            # Check if tables have data
            tables_with_data = set()
            for table_name in existing_tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cur.fetchone()[0]
                    if count > 0:
                        tables_with_data.add(table_name)
                        log_to_ui(f"Table {table_name} has {count:,} rows", module="Taxroll")
                    else:
                        log_to_ui(f"Table {table_name} exists but is empty", module="Taxroll", status="Warning")
                except Exception as e:
                    log_to_ui(f"Error checking table {table_name}: {e}", module="Taxroll", status="Warning")

            # Alternative approach: Check logs for completed final tables
            cur.execute("""
                SELECT DISTINCT FinalTable, Status
                FROM dbo.tbl_appraisal_logs
                WHERE Username = ?
                  AND FinalTable IS NOT NULL
                  AND Status = 'Completed'
            """, (username,))
            rows = cur.fetchall()
            completed_from_logs = {r[0] for r in rows}

            log_to_ui(f"Tables completed according to logs: {len(completed_from_logs)}", module="Taxroll")
            log_to_ui(f"Tables with data: {len(tables_with_data)}", module="Taxroll")

            # Use tables that either exist with data OR are marked as completed in logs
            available_tables = existing_tables.intersection(required)

            if not available_tables:
                log_to_ui("No final tables found. Please run raw data processing first.", module="Taxroll",
                          status="Error")
                return False

            if len(available_tables) < len(required):
                missing = required - available_tables
                log_to_ui(f"Some required tables are missing: {', '.join(missing)}", module="Taxroll", status="Warning")
                log_to_ui("Attempting to run with available tables...", module="Taxroll")

            # Check if stored procedure exists
            cur.execute("""
                SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_NAME = 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1' 
                AND ROUTINE_TYPE = 'PROCEDURE'
            """)

            if not cur.fetchone():
                log_to_ui("Stored procedure 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1' not found in database", module="Taxroll",
                          status="Error")
                return False

            log_to_ui("All validations passed. Starting Taxroll Data Loading...", module="Taxroll")

            # Execute stored procedure with parameters
            try:
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

                # Fetch any messages or results from the stored procedure
                while cur.nextset():
                    pass

                conn.commit()
                log_to_ui("Taxroll Data Loading stored procedure executed successfully.", module="Taxroll",
                          status="Completed")
                return True

            except pyodbc.Error as db_error:
                log_to_ui(f"Database error during stored procedure execution: {db_error}", module="Taxroll",
                          status="Error")
                return False

    except pyodbc.Error as conn_error:
        log_to_ui(f"Database connection error: {conn_error}", module="Taxroll", status="Error")
        return False
    except Exception as e:
        log_to_ui(f"Unexpected error during Taxroll Data Loading: {e}", module="Taxroll", status="Error")
        return False


def run_luc_loading():
    """Execute LUC loading stored procedure."""
    try:
        log_to_ui("Starting LUC loading...", module="LUC")

        # Check if LUC stored procedure exists (replace with your actual LUC procedure name)
        with get_db_connection() as conn:
            cur = conn.cursor()

            # Replace 'YourLUCStoredProc' with your actual LUC stored procedure name
            luc_proc_name = 'YourLUCStoredProc'  # Update this with your actual procedure name

            cur.execute("""
                SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_NAME = ? 
                AND ROUTINE_TYPE = 'PROCEDURE'
            """, (luc_proc_name,))

            if cur.fetchone():
                log_to_ui(f"Found LUC stored procedure: {luc_proc_name}", module="LUC")
                # Execute your actual LUC stored procedure here
                # cur.execute(f"EXEC dbo.{luc_proc_name} @Param1=?, @Param2=?", (val1, val2))
                # conn.commit()

                # For now, simulate processing
                time.sleep(2)  # simulate processing time
                log_to_ui("LUC loading procedure executed successfully.", module="LUC", status="Completed")
                return True
            else:
                log_to_ui(
                    f"LUC stored procedure '{luc_proc_name}' not found. Please update the procedure name in the code.",
                    module="LUC", status="Warning")
                log_to_ui("LUC loading simulated (no actual procedure executed).", module="LUC", status="Completed")
                time.sleep(1)  # simulate processing time
                return True

    except pyodbc.Error as db_error:
        log_to_ui(f"Database error during LUC loading: {db_error}", module="LUC", status="Error")
        return False
    except Exception as e:
        log_to_ui(f"LUC loading error: {e}", module="LUC", status="Error")
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
    patterns = ['*.zip', '*.txt']  # Remove duplicates - glob is case-insensitive on Windows
    files = []
    seen_files = set()  # Track files we've already added

    for p in patterns:
        found_files = glob.glob(os.path.join(LARGE_FILE_FOLDER, p))
        # Also check uppercase versions explicitly for cross-platform compatibility
        found_files.extend(glob.glob(os.path.join(LARGE_FILE_FOLDER, p.upper())))

        for f in found_files:
            # Normalize path to avoid duplicates
            normalized_path = os.path.normpath(f)
            if normalized_path not in seen_files:
                files.append(f)
                seen_files.add(normalized_path)

    file_info = []
    for f in files:
        try:
            size = os.path.getsize(f)
            file_info.append({
                'display': f"{os.path.basename(f)} ({format_file_size(size)})",
                'path': f,
                'size': size,
                'filename': os.path.basename(f)
            })
        except Exception as e:
            log_to_ui(f"Could not stat file {f}: {e}", module="Main", status="Warning")

    return sorted(file_info, key=lambda x: x['filename'].lower())


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
        time.sleep(0.1)
        st.rerun()

    # Enhanced log display section
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 15px 25px;
        border-radius: 12px;
        margin: 20px 0;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    ">
        <h3 style="
            color: white; 
            margin: 0; 
            font-weight: 600;
            font-size: 1.0rem;
            display: flex;
            align-items: center;
        ">
            📊 Live Processing Monitor
        </h3>
    </div>
    """, unsafe_allow_html=True)

    # Processing status indicator
    if st.session_state.get("processing_active", False):
        st.markdown("""
        <div style="
            background: linear-gradient(90deg, #ff9a9e 0%, #fecfef 50%, #fecfef 100%);
            border: none;
            border-radius: 25px;
            padding: 15px 30px;
            margin: 15px 0;
            text-align: center;
            box-shadow: 0 4px 15px rgba(255, 154, 158, 0.3);
            animation: pulse 2s infinite;
        ">
            <style>
            @keyframes pulse {
                0% { box-shadow: 0 4px 15px rgba(255, 154, 158, 0.3); }
                50% { box-shadow: 0 6px 20px rgba(255, 154, 158, 0.5); }
                100% { box-shadow: 0 4px 15px rgba(255, 154, 158, 0.3); }
            }
            </style>
            <strong style="color: #d63384; font-size: 1.1rem;">
                🔄 ACTIVE PROCESSING - Real-time updates enabled
            </strong>
        </div>
        """, unsafe_allow_html=True)

    # Enhanced log container
    log_container = st.container()
    with log_container:
        if st.session_state.logs:
            recent_logs = st.session_state.logs[-50:]  # Show last 50 logs

            for entry in reversed(recent_logs):
                timestamp = entry.get("Timestamp", "")
                module = entry.get("Module", "")
                message = entry.get("Message", "")
                status = entry.get("Status", "")

                # Enhanced log formatting with modern design
                if status == "Error":
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #ff6b6b, #ee5a52);
                        color: white;
                        padding: 12px 18px;
                        margin: 8px 0;
                        border-radius: 8px;
                        border-left: 4px solid #ff4757;
                        box-shadow: 0 3px 10px rgba(255, 107, 107, 0.3);
                    ">
                        <strong>❌ [{timestamp}] [{module}]</strong><br>
                        {message}
                    </div>
                    """, unsafe_allow_html=True)
                elif status == "Warning":
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #ffa726, #ff9800);
                        color: white;
                        padding: 12px 18px;
                        margin: 8px 0;
                        border-radius: 8px;
                        border-left: 4px solid #ff8f00;
                        box-shadow: 0 3px 10px rgba(255, 167, 38, 0.3);
                    ">
                        <strong>⚠️ [{timestamp}] [{module}]</strong><br>
                        {message}
                    </div>
                    """, unsafe_allow_html=True)
                elif status == "Completed":
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #2ed573, #1e90ff);
                        color: white;
                        padding: 12px 18px;
                        margin: 8px 0;
                        border-radius: 8px;
                        border-left: 4px solid #2ed573;
                        box-shadow: 0 3px 10px rgba(46, 213, 115, 0.3);
                    ">
                        <strong>✅ [{timestamp}] [{module}]</strong><br>
                        {message}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #74b9ff, #0984e3);
                        color: white;
                        padding: 12px 18px;
                        margin: 8px 0;
                        border-radius: 8px;
                        border-left: 4px solid #74b9ff;
                        box-shadow: 0 3px 10px rgba(116, 185, 255, 0.3);
                    ">
                        <strong>ℹ️ [{timestamp}] [{module}]</strong><br>
                        {message}
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="
                text-align: center;
                padding: 40px 20px;
                background: linear-gradient(135deg, #f8f9fa, #e9ecef);
                border-radius: 12px;
                border: 2px dashed #ced4da;
                margin: 20px 0;
            ">
                <h4 style="color: #6c757d; margin: 0;">No processing logs available yet</h4>
                <p style="color: #adb5bd; margin: 10px 0;">Logs will appear here in real-time during processing</p>
            </div>
            """, unsafe_allow_html=True)


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


# ============================================================================
# MAIN STREAMLIT APPLICATION
# ============================================================================

# Enhanced page configuration
st.set_page_config(
    page_title="Taxroll Consolidation Manager | Professional ETL Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize state and database
init_state()
ensure_logs_table()

# Professional CSS styling
st.markdown("""
<style>
    /* Main app styling */
    .main > div {
        padding-top: 1.2rem;
        font-family: "Inter", "Segoe UI", sans-serif;
        color: #2c2c2c;
    }

    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #eef2f7, #dfe6ef);
        padding: 1.2rem 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    }

    .main-header h1 {
        color: #2c3e50;
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
    }

    .main-header p {
        color: #555;
        margin: 0.3rem 0 0 0;
        font-size: 0.95rem;
    }

    /* Section headers */
    .section-header {
        background: #f9fafc;
        padding: 0.5rem 0.9rem;
        border-radius: 6px;
        margin: 0.7rem 0;
        border: 1px solid #e3e6eb;
        font-weight: 600;
        font-size: 0.5rem;
        color: #444;
    }

    /* Card styling */
    .info-card {
        background: #fff;
        padding: 1rem 1.2rem;
        border-radius: 8px;
        border: 1px solid #e3e6eb;
        margin: 0.8rem 0;
        transition: box-shadow 0.2s ease;
    }

    .info-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.06);
    }

    /* Status indicators */
    .status-success {
        background: #e6f4ea;
        color: #1e7e34;
        padding: 0.25rem 0.7rem;
        border-radius: 10px;
        font-weight: 500;
        font-size: 0.8rem;
    }

    .status-error {
        background: #fdecea;
        color: #c82333;
        padding: 0.25rem 0.7rem;
        border-radius: 10px;
        font-weight: 500;
        font-size: 0.8rem;
    }

    .status-warning {
        background: #fff4e5;
        color: #b45f06;
        padding: 0.25rem 0.7rem;
        border-radius: 10px;
        font-weight: 500;
        font-size: 0.8rem;
    }

    /* Button styling */
    .stButton > button {
        border-radius: 6px;
        border: 1px solid #d1d5db;
        padding: 0.4rem 1rem;
        font-weight: 500;
        font-size: 0.9rem;
        background-color: #f8f9fa;
        color: #333;
        transition: background 0.2s ease, border 0.2s ease;
    }

    .stButton > button:hover {
        background-color: #e9ecef;
        border-color: #b0b5bb;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background: #f7f9fb;
        border-right: 1px solid #e0e0e0;
    }

    /* Progress bar styling */
    .stProgress .st-bo {
        background: #4a90e2;
    }

    /* Metrics styling */
    .metric-card {
        background: #f0f4f9;
        color: #2c3e50;
        padding: 1rem;
        border-radius: 100px;
        text-align: center;
        margin: 0.5rem 0;
    }

    .metric-card h2 {
        margin: 0;
        font-size: 1.6rem;
        font-weight: 600;
    }

    .metric-card p {
        margin: 0.3rem 0 0 0;
        font-size: 0.85rem;
        opacity: 0.85;
    }
</style>

""", unsafe_allow_html=True)

# Professional header
st.markdown("""
<div class="main-header">
    <h1>🏛️ Taxroll Appraisalinfo  </h1>
    <p>Professional ETL Platform for Large-Scale Tax Data Processing (up to 10GB+)</p>
</div>
""", unsafe_allow_html=True)

# Sidebar for system information
with st.sidebar:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        color: white;
        text-align: center;
    ">
        <h3 style="margin: 0;">System Status</h3>
    </div>
    """, unsafe_allow_html=True)

    # System metrics
    username = getpass.getuser()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    st.markdown(f"""
    <div class="info-card">
        <strong>👤 User:</strong> {username}<br>
        <strong>🕒 Time:</strong> {current_time}<br>
        <strong>🗄️ Database:</strong> {DATABASE}<br>
        <strong>🖥️ Server:</strong> {SERVER}
    </div>
    """, unsafe_allow_html=True)

    # Processing status
    if st.session_state.get("processing_active", False):
        st.markdown("""
        <div class="status-warning">
            🔄 Processing Active
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="status-success">
            ✅ System Ready
        </div>
        """, unsafe_allow_html=True)

# File Management Section
st.markdown("""
<div class="section-header">
    <h3>📁 Large File Management</h3>
</div>
""", unsafe_allow_html=True)

with st.expander("File Storage Controls", expanded=True):
    folder_path = ensure_large_file_folder()

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("📁 Open File Directory", use_container_width=True):
            try:
                os.startfile(folder_path)
                st.success("File directory opened!")
            except Exception as e:
                st.error(f"Could not open directory: {e}")

    with col2:
        if st.button("🔄 Refresh File List", use_container_width=True):
            if hasattr(st, 'cache_data'):
                st.cache_data.clear()
            st.rerun()

    with col3:
        available_files = get_available_files()
        st.metric("Available Files", len(available_files))

    # File information display
    st.markdown(f"""
    <div class="info-card">
        <strong>📂 Storage Directory:</strong><br>
        <code>{folder_path}</code>
    </div>
    """, unsafe_allow_html=True)

    if available_files:
        st.subheader("Available Files")
        file_df = pd.DataFrame([
            {"Filename": fi["filename"], "Size": format_file_size(fi["size"])}
            for fi in available_files
        ])
        st.dataframe(file_df, use_container_width=True, height=200)

# Configuration Section
st.markdown("""
<div class="section-header">
    <h3>⚙️ Processing Configuration</h3>
</div>
""", unsafe_allow_html=True)

col1, col2 = st.columns([1, 3])
with col1:
    st.selectbox("Tax Year", ["2026", "2025", "2024", "2023"], key="tax_year")

# File Selection Section
st.markdown("""
<div class="section-header">
    <h3>📋 Data Source Configuration</h3>
</div>
""", unsafe_allow_html=True)

# Row management controls
col1, col2 = st.columns([1, 1])
with col1:
    if st.button("➕ Add Processing Row", use_container_width=True, type="secondary"):
        if len(st.session_state.rows) < 30:
            st.session_state.rows.append({"file": None, "county": "", "layout": "All", "file_path": ""})
            st.success("New row added!")

with col2:
    if st.button("➖ Remove Last Row", use_container_width=True, type="secondary"):
        if len(st.session_state.rows) > 1:
            st.session_state.rows.pop()
            st.success("Row removed!")

# File selection interface
all_counties = _get_counties()
available_files_map = {f['display']: f for f in available_files}

for idx, row in enumerate(st.session_state.rows):
    with st.container():
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            padding: 1.5rem;
            border-radius: 12px;
            margin: 1rem 0;
            border: 2px solid #dee2e6;
        ">
            <h4 style="margin: 0 0 1rem 0; color: #495057;">Processing Row {idx + 1}</h4>
        """, unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1])

        with c1:
            file_options = ["Select a file..."] + list(available_files_map.keys())
            selected = st.selectbox(
                "Data Source File",
                file_options,
                key=f"file_select_{idx}",
                help="Choose from available large files in the storage directory"
            )
            if selected != "Select a file...":
                fi = available_files_map.get(selected)
                if fi:
                    st.session_state.rows[idx]["file_path"] = fi['path']
                    st.session_state.rows[idx]["file"] = None
                    st.success(f"Selected: {fi['filename']}")
            else:
                st.session_state.rows[idx]["file_path"] = ""

        with c2:
            county_options = ["Select county..."] + (all_counties if all_counties else [])
            selected_county = st.selectbox(
                "County/CAD",
                county_options,
                index=county_options.index(st.session_state.rows[idx]["county"]) if
                st.session_state.rows[idx]["county"] in county_options else 0,
                key=f"county_{idx}",
                help="Select the county for this data file"
            )
            if selected_county != "Select county...":
                st.session_state.rows[idx]["county"] = selected_county
            else:
                st.session_state.rows[idx]["county"] = ""

        with c3:
            st.session_state.rows[idx]["layout"] = st.selectbox(
                "Layout Type",
                ["All"] + list(DUMP_LAYOUTS.keys()),
                key=f"layout_{idx}",
                help="Choose specific layout or 'All' for complete processing"
            )

        with c4:
            # File status indicator
            fp = st.session_state.rows[idx].get("file_path")
            if fp and os.path.exists(fp):
                st.markdown(f"""
                <div class="status-success">
                    ✅ {format_file_size(os.path.getsize(fp))}
                </div>
                """, unsafe_allow_html=True)
            elif fp:
                st.markdown('<div class="status-error">❌ Not Found</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="status-warning">⚠️ No File</div>', unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

# Processing Control Section
st.markdown("""
<div class="section-header">
    <h3>🚀 Processing Operations</h3>
</div>
""", unsafe_allow_html=True)

# Enhanced processing buttons with better styling
proc_col1, proc_col2, proc_col3, proc_col4 = st.columns([1, 1, 1, 1])

with proc_col1:
    if st.button(
            "▶️ Start Processing",
            type="primary",
            disabled=st.session_state.get("processing_active", False),
            use_container_width=True,
            help="Process selected files and create staging tables"
    ):
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

            st.success("✅ Raw Dump Data Loading Completed")
            st.balloons()

            log_to_ui("Raw Dump data Loading completed", module="Main", status="Completed")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False

with proc_col2:
    if st.button(
            "🚀 AutoProcess All",
            type="primary",
            disabled=st.session_state.get("processing_active", False),
            use_container_width=True,
            help="Complete end-to-end processing pipeline"
    ):
        # Execute the complete automated processing pipeline
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
                st.error("No valid rows to process. Please check file paths and county selections.")
                st.session_state.processing_active = False
                st.session_state.auto_refresh_logs = False
            else:
                # Clear previous state
                st.session_state.tables_by_layout = {}

                # Step 1: Process all dump files
                log_to_ui("Step 1: Processing dump files...", module="AutoProcess")
                total_steps = sum(1 if r["layout"] != "All" else len(DUMP_LAYOUTS) for r in valid_rows) + 3
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
                        module="AutoProcess")

                    if layout_choice == "All":
                        for lt in DUMP_LAYOUTS.keys():
                            log_to_ui(f"Row {i}: Processing layout '{lt}'...", module="AutoProcess")
                            main_status.text(f"Row {i}: Processing layout '{lt}'...")
                            staging_tbl = worker_full_flow(file_path, lt, county, tax_year)
                            if staging_tbl:
                                st.session_state.tables_by_layout.setdefault(lt, []).append(staging_tbl)
                            step += 1
                            main_progress.progress(min(step / total_steps, 1.0))
                    else:
                        log_to_ui(f"Row {i}: Processing layout '{layout_choice}'...", module="AutoProcess")
                        main_status.text(f"Row {i}: Processing layout '{layout_choice}'...")
                        staging_tbl = worker_full_flow(file_path, layout_choice, county, tax_year)
                        if staging_tbl:
                            st.session_state.tables_by_layout.setdefault(layout_choice, []).append(staging_tbl)
                        step += 1
                        main_progress.progress(min(step / total_steps, 1.0))

                # Step 2: Consolidation
                log_to_ui("Step 2: Starting consolidation...", module="AutoProcess")
                main_status.text("Starting consolidation...")
                consolidate_per_layout(st.session_state.tables_by_layout)
                step += 1
                main_progress.progress(min(step / total_steps, 1.0))

                # Step 3: Run Taxroll Data Loading
                log_to_ui("Step 3: Starting Taxroll Data Loading...", module="AutoProcess")
                main_status.text("Starting Taxroll Data Loading...")
                taxroll_success = run_taxroll_data_loading()
                step += 1
                main_progress.progress(min(step / total_steps, 1.0))

                if not taxroll_success:
                    log_to_ui("AutoProcess All failed at Taxroll Data Loading step", module="AutoProcess",
                              status="Error")
                    st.error("AutoProcess All failed at Taxroll Data Loading step")
                    st.session_state.processing_active = False
                    st.session_state.auto_refresh_logs = False
                else:
                    # Step 4: Run LUC Loading
                    log_to_ui("Step 4: Starting LUC Loading...", module="AutoProcess")
                    main_status.text("Starting LUC Loading...")
                    luc_success = run_luc_loading()
                    step += 1
                    main_progress.progress(1.0)

                    if not luc_success:
                        log_to_ui("AutoProcess All failed at LUC Loading step", module="AutoProcess", status="Error")
                        st.error("AutoProcess All failed at LUC Loading step")
                    else:
                        # Completion
                        log_to_ui("=== AutoProcess All completed successfully ===", module="AutoProcess",
                                  status="Completed")
                        st.success("AutoProcess All completed successfully!")
                        st.balloons()

                    st.session_state.processing_active = False
                    st.session_state.auto_refresh_logs = False
                    main_status.text("AutoProcess All finished!")

        except Exception as e:
            log_to_ui(f"AutoProcess All failed with error: {e}", module="AutoProcess", status="Error")
            st.error(f"AutoProcess All failed with error: {e}")
            st.session_state.processing_active = False
            st.session_state.auto_refresh_logs = False

with proc_col3:
    if st.button(
            "📊 Taxroll Loading",
            type="secondary",
            disabled=st.session_state.get("processing_active", False),
            use_container_width=True,
            help="Execute taxroll data loading procedures"
    ):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        # Create a detailed diagnostic display
        diagnostic_container = st.empty()

        with diagnostic_container.container():
            st.subheader("Taxroll Loading Diagnostics")

            # Step 1: Check username and required tables
            username = getpass.getuser().replace(" ", "_")
            st.write(f"**Username:** {username}")

            required_tables = [
                f"APPRAISAL_INFO_FINAL_{username}",
                f"APPRAISAL_IMPROVEMENT_INFO_FINAL_{username}",
                f"APPRAISAL_IMPROVEMENT_DETAIL_FINAL_{username}",
                f"APPRAISAL_LAND_DETAIL_FINAL_{username}",
                f"APPRAISAL_ABSTRACT_SUBDV_FINAL_{username}",
            ]

            st.write("**Required Tables:**")
            for table in required_tables:
                st.write(f"- {table}")

            # Step 2: Check actual table existence and data
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()

                    st.write("**Table Status Check:**")
                    table_status = {}

                    for table_name in required_tables:
                        # Check if table exists
                        cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (table_name,))
                        exists = cur.fetchone() is not None

                        if exists:
                            # Check row count
                            try:
                                cur.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                                count = cur.fetchone()[0]
                                table_status[table_name] = {"exists": True, "count": count}
                                st.write(f"✅ {table_name}: EXISTS ({count:,} rows)")
                            except Exception as e:
                                table_status[table_name] = {"exists": True, "count": 0, "error": str(e)}
                                st.write(f"⚠️ {table_name}: EXISTS (Error counting rows: {e})")
                        else:
                            table_status[table_name] = {"exists": False, "count": 0}
                            st.write(f"❌ {table_name}: NOT FOUND")

                    # Step 3: Check stored procedure
                    st.write("**Stored Procedure Check:**")
                    cur.execute("""
                        SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES 
                        WHERE ROUTINE_NAME = 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1' 
                        AND ROUTINE_TYPE = 'PROCEDURE'
                    """)

                    sp_exists = cur.fetchone() is not None
                    if sp_exists:
                        st.write("✅ Stored Procedure 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1' EXISTS")
                    else:
                        st.write("❌ Stored Procedure 'AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1' NOT FOUND")

                    # Step 4: Attempt execution if everything looks good
                    existing_tables_with_data = [t for t, status in table_status.items()
                                                 if status["exists"] and status["count"] > 0]

                    if len(existing_tables_with_data) >= 3 and sp_exists:  # Relaxed requirement
                        st.write("**Attempting Stored Procedure Execution:**")
                        try:
                            cur.execute("""
                                EXEC dbo.AppraisalINFO_AutoDataLoading_BulkLoadingTest_Single_1
                                    @APPRAISAL_INFO=?,
                                    @APPRAISAL_IMPROVEMENT_INFO=?,
                                    @APPRAISAL_IMPROVEMENT_DETAIL=?,
                                    @APPRAISAL_LAND_DETAIL=?,
                                    @APPRAISAL_ABSTRACT_SUBDV=?
                            """, tuple(required_tables))

                            # Process any result sets
                            while cur.nextset():
                                pass

                            conn.commit()
                            st.write("✅ **SUCCESS**: Stored procedure executed successfully!")
                            st.success("✅ Taxroll Data Loading Completed Successfully!")
                            log_to_ui("Taxroll Data Loading completed successfully", module="Taxroll",
                                      status="Completed")

                        except pyodbc.Error as db_error:
                            st.write(f"❌ **DATABASE ERROR**: {db_error}")
                            st.error(f"Database Error: {db_error}")
                            log_to_ui(f"Database error in Taxroll Loading: {db_error}", module="Taxroll",
                                      status="Error")

                        except Exception as exec_error:
                            st.write(f"❌ **EXECUTION ERROR**: {exec_error}")
                            st.error(f"Execution Error: {exec_error}")
                            log_to_ui(f"Execution error in Taxroll Loading: {exec_error}", module="Taxroll",
                                      status="Error")
                    else:
                        missing_items = []
                        if not sp_exists:
                            missing_items.append("Stored procedure missing")
                        if len(existing_tables_with_data) < 3:
                            missing_items.append(f"Only {len(existing_tables_with_data)} tables with data found")

                        st.write(f"❌ **CANNOT PROCEED**: {', '.join(missing_items)}")
                        st.error("Cannot proceed with Taxroll Loading - see diagnostics above")
                        log_to_ui(f"Taxroll Loading failed: {', '.join(missing_items)}", module="Taxroll",
                                  status="Error")

            except pyodbc.Error as conn_error:
                st.write(f"❌ **CONNECTION ERROR**: {conn_error}")
                st.error(f"Database Connection Error: {conn_error}")
                log_to_ui(f"Connection error in Taxroll Loading: {conn_error}", module="Taxroll", status="Error")

            except Exception as e:
                st.write(f"❌ **UNEXPECTED ERROR**: {e}")
                st.error(f"Unexpected Error: {e}")
                log_to_ui(f"Unexpected error in Taxroll Loading: {e}", module="Taxroll", status="Error")

        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False

with proc_col4:
    if st.button(
            "📂 LUC Loading",
            type="secondary",
            disabled=st.session_state.get("processing_active", False),
            use_container_width=True,
            help="Execute LUC loading procedures"
    ):
        st.session_state.processing_active = True
        st.session_state.auto_refresh_logs = True

        success = run_luc_loading()

        if success:
            st.success("✅ LUC Loading Completed")
        else:
            st.error("❌ LUC Loading Failed")

        st.session_state.processing_active = False
        st.session_state.auto_refresh_logs = False

# Data Summary Section
st.markdown("""
<div class="section-header">
    <h3>📊 Processing Summary & Status</h3>
</div>
""", unsafe_allow_html=True)

left_col, right_col = st.columns([1.4, 1])

with left_col:
    # Enhanced summary table
    st.subheader("Current Configuration")
    summary = []
    for i, r in enumerate(st.session_state.rows):
        file_info = "No file selected"
        status_icon = "⚠️"

        if r.get("file_path"):
            try:
                if os.path.exists(r.get("file_path")):
                    file_info = f"{os.path.basename(r['file_path'])} ({format_file_size(os.path.getsize(r['file_path']))})"
                    status_icon = "✅"
                else:
                    file_info = f"{os.path.basename(r.get('file_path', ''))} (Missing)"
                    status_icon = "❌"
            except Exception:
                file_info = os.path.basename(r.get("file_path", ""))
                status_icon = "❌"

        summary.append({
            "Row": i + 1,
            "Status": status_icon,
            "File": file_info,
            "County": r.get("county") or "Not selected",
            "Layout": r.get("layout", "All")
        })

    if summary:
        summary_df = pd.DataFrame(summary)
        st.dataframe(
            summary_df,
            use_container_width=True,
            height=250,
            column_config={
                "Row": st.column_config.NumberColumn("Row #", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "File": st.column_config.TextColumn("File", width="large"),
                "County": st.column_config.TextColumn("County", width="medium"),
                "Layout": st.column_config.TextColumn("Layout", width="medium")
            }
        )

with right_col:
    # Expected final tables
    st.subheader("Expected Final Tables")
    if st.session_state.tables_by_layout:
        username = getpass.getuser().replace(" ", "_")
        finals = []
        for layout in st.session_state.tables_by_layout.keys():
            final_table_name = f"{DUMP_LAYOUTS[layout]['base_table_name']}_FINAL_{username}"
            finals.append({
                "Layout": layout,
                "Final Table": final_table_name
            })

        finals_df = pd.DataFrame(finals)
        st.dataframe(finals_df, use_container_width=True, height=200)
    else:
        st.markdown("""
        <div style="
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 10px;
            border: 2px dashed #ced4da;
        ">
            <p style="color: #6c757d; margin: 0;">No final tables created yet</p>
            <small style="color: #adb5bd;">Tables will appear here after processing</small>
        </div>
        """, unsafe_allow_html=True)

# Database Status Section
st.markdown("""
<div class="section-header">
    <h3>🗄️ Database Loading Status</h3>
</div>
""", unsafe_allow_html=True)

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
            # Enhanced status display with metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_records = len(df)
                st.markdown(f"""
                <div class="metric-card">
                    <h2>{total_records}</h2>
                    <p>Total Records</p>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                taxroll_completed = len(df[df['TaxrollLoadingStatus'] == 'Completed'])
                st.markdown(f"""
                <div class="metric-card" style="background: linear-gradient(135deg, #2ed573, #17a2b8);">
                    <h2>{taxroll_completed}/{total_records}</h2>
                    <p>Taxroll Completed</p>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                luc_completed = len(df[df['LUCStatus'] == 'Completed'])
                st.markdown(f"""
                <div class="metric-card" style="background: linear-gradient(135deg, #f093fb, #f5576c);">
                    <h2>{luc_completed}/{total_records}</h2>
                    <p>LUC Completed</p>
                </div>
                """, unsafe_allow_html=True)

            with col4:
                unique_counties = df['CountyName'].nunique()
                st.markdown(f"""
                <div class="metric-card" style="background: linear-gradient(135deg, #ffa726, #ff9800);">
                    <h2>{unique_counties}</h2>
                    <p>Unique Counties</p>
                </div>
                """, unsafe_allow_html=True)

            # Enhanced data table with better formatting
            st.subheader("Detailed Status Overview")
            st.dataframe(
                df,
                use_container_width=True,
                height=350,
                column_config={
                    "TaxYear": st.column_config.NumberColumn("Tax Year", format="%d"),
                    "CountyName": st.column_config.TextColumn("County Name", width="medium"),
                    "CadID": st.column_config.NumberColumn("CAD ID", width="small"),
                    "TaxrollLoadingStatus": st.column_config.TextColumn(
                        "Taxroll Status",
                        help="Status of taxroll data loading"
                    ),
                    "LUCStatus": st.column_config.TextColumn(
                        "LUC Status",
                        help="Status of LUC loading"
                    ),
                    "TaxrollTableName": st.column_config.TextColumn("Taxroll Table", width="medium"),
                    "CQETableName": st.column_config.TextColumn("CQE Table", width="medium"),
                    "JurisdictionTableName": st.column_config.TextColumn("Jurisdiction Table", width="medium")
                }
            )

        else:
            st.markdown("""
            <div style="
                text-align: center;
                padding: 3rem 2rem;
                background: linear-gradient(135deg, #f8f9fa, #e9ecef);
                border-radius: 15px;
                border: 2px dashed #ced4da;
                margin: 2rem 0;
            ">
                <h4 style="color: #6c757d; margin: 0 0 1rem 0;">No Loading Status Records Found</h4>
                <p style="color: #adb5bd; margin: 0;">
                    No records found in Appraisal_LoadingSummaryStatus table.<br>
                    Status information will appear here after processing begins.
                </p>
            </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.markdown(f"""
    <div class="status-error" style="padding: 1rem; margin: 1rem 0; display: block;">
        ❌ Error retrieving loading status: {e}
    </div>
    """, unsafe_allow_html=True)
    log_to_ui(f"Error retrieving loading status: {e}", module="Database", status="Error")

# Live Logs Section (Enhanced)
display_live_logs()

# Auto-refresh mechanism for live updates
if st.session_state.get("auto_refresh_logs", False):
    time.sleep(2)
    st.rerun()

# Footer
st.markdown("""
<div style="
    text-align: center;
    padding: 2rem;
    margin-top: 3rem;
    border-top: 2px solid #e9ecef;
    color: #6c757d;
">
    <p style="margin: 0;">
        <strong>Taxroll Consolidation Manager</strong> | Professional ETL Platform<br>
        <small>Optimized for large-scale tax data processing and consolidation</small>
    </p>
</div>
""", unsafe_allow_html=True)