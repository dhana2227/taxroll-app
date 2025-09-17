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
        st.session_state.rows = [{"file": None, "server_path": "", "county": "", "layout": "All"}]
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "tables_by_layout" not in st.session_state:
        st.session_state.tables_by_layout = {}  # layout -> [temp tables]
    if "tax_year" not in st.session_state:
        st.session_state.tax_year = "2026"

def log_to_ui(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.logs.append(f"[{ts}] {msg}")

def render_logs():
    if st.session_state.get("logs"):
        st.text_area("Live Log", value="\n".join(st.session_state.logs), height=220)

# ---------------- DB helpers ----------------
def get_db_connection():
    return pyodbc.connect(CONN_STR)

# (other helper functions unchanged...)

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="Taxroll Consolidation Manager", layout="wide")
init_state()

# ---------------- Fetch counties list ----------------
try:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT county FROM YourCountyTable ORDER BY county")
        all_counties = [row[0] for row in cursor.fetchall()]
except Exception as e:
    all_counties = ["Harris", "Dallas", "Travis", "Bexar"]  # fallback list
    st.warning(f"Could not fetch counties from DB, using fallback: {e}")

st.title("Taxroll Consolidation Manager")
st.caption("ETL + CADID update + Per-layout consolidation (Streamlit UI)")

# ... rest of your code (unchanged) ...
# ... top-level tax year and county fetching code unchanged ...

left, right = st.columns([1.3, 1])

# ---------- LEFT: Rows (inputs) ----------
with left:
    st.subheader("Inputs")
    addcol, rmcol = st.columns([1,1])
    with addcol:
        if st.button("➕ Add Row", use_container_width=True):
            if len(st.session_state.rows) < 30:
                st.session_state.rows.append({"file": None, "server_path": "", "county": "", "layout": "All"})
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
            c1, c2, c3, c4 = st.columns([1.2, 1.2, 1, 1])
            with c1:
                uploaded = st.file_uploader(
                    "ZIP or TXT (≤2 GB via config)",
                    type=["zip", "txt", "TXT", "ZIP"],
                    key=f"file_{idx}"
                )
                st.session_state.rows[idx]["file"] = uploaded
            with c2:
                st.session_state.rows[idx]["server_path"] = st.text_input(
                    "Or server file path", value=row.get("server_path", ""), key=f"srv_{idx}"
                )
            with c3:
                st.session_state.rows[idx]["county"] = st.selectbox(
                    "County", all_counties, index=0 if all_counties else None, key=f"county_{idx}"
                ) if all_counties else ""
            with c4:
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
            "Server Path": r["server_path"] or "(none)",
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

    for i, r in enumerate(st.session_state.rows, start=1):
        file = r["file"]
        server_path = r["server_path"]
        county = r["county"]
        layout_choice = r["layout"]
        tax_year = st.session_state.tax_year

        if not (file or server_path) or not county or not layout_choice:
            log_to_ui(f"Row {i}: Missing inputs. Skipping.")
            step += 1
            progress.progress(min(step/total_steps, 1.0))
            continue

        try:
            if server_path and os.path.exists(server_path):
                tmp_path = server_path
                log_to_ui(f"Row {i}: Using server file path {tmp_path}")
            else:
                suffix = ".zip" if file.name.lower().endswith(".zip") else ".txt"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(file.getbuffer())
                    tmp_path = tmp.name
                log_to_ui(f"Row {i}: Uploaded file saved to temp: {tmp_path}")
        except Exception as e:
            log_to_ui(f"Row {i}: Could not resolve file ({e}). Skipping.")
            step += 1
            progress.progress(min(step/total_steps, 1.0))
            continue

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

        if not server_path:  # only cleanup temp if it was uploaded
            try:
                os.unlink(tmp_path)
            except:
                pass

    log_to_ui("Starting consolidation for all layouts...")
    consolidate_per_layout(st.session_state.tables_by_layout)
    step += 1
    progress.progress(1.0)

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
