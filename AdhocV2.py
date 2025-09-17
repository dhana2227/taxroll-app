import streamlit as st
import pyodbc
import pandas as pd
import ctypes

# ---------------- CONFIG ----------------
SERVERS = [
    {"name": "Primary",
     "conn": r"DRIVER={ODBC Driver 13 for SQL Server};SERVER=taxrollstage-db;DATABASE=TaxrollStaging;Trusted_Connection=yes;"},
    {"name": "Secondary",
     "conn": r"DRIVER={ODBC Driver 13 for SQL Server};SERVER=second-db;DATABASE=TaxrollStaging;Trusted_Connection=yes;"}
]


# ---------------- WINDOWS LOGIN ----------------
def verify_windows_login(username, password):
    logon32_logon_interactive = 2
    logon32_provider_default = 0
    token = ctypes.wintypes.HANDLE()
    result = ctypes.windll.advapi32.LogonUserW(
        username, None, password,
        logon32_logon_interactive,
        logon32_provider_default,
        ctypes.byref(token)
    )
    return result != 0


# ---------------- DATABASE UPSERT ----------------
def upsert_to_sql(df, table_name, module):
    """Run MERGE UPSERT for each module's table"""
    for server in SERVERS:
        conn = pyodbc.connect(server["conn"])
        cursor = conn.cursor()

        for _, row in df.iterrows():
            if module == "Value Update":
                cursor.execute(f"""
                    MERGE {table_name} AS target
                    USING (SELECT ? AS CADID, ? AS TaxYear, ? AS AccountNumber, ? AS UpdatedValue) AS src
                    ON target.CADID = src.CADID AND target.AccountNumber = src.AccountNumber
                    WHEN MATCHED THEN UPDATE SET UpdatedValue = src.UpdatedValue
                    WHEN NOT MATCHED THEN INSERT (CADID, TaxYear, AccountNumber, UpdatedValue)
                    VALUES (src.CADID, src.TaxYear, src.AccountNumber, src.UpdatedValue);
                """, row.CADID, row.TaxYear, row.AccountNumber, row.UpdatedValue)

            elif module == "LUC Update":
                cursor.execute(f"""
                    MERGE {table_name} AS target
                    USING (SELECT ? AS CADID, ? AS TaxYear, ? AS AccountNumber, ? AS UpdatedLUC) AS src
                    ON target.CADID = src.CADID AND target.AccountNumber = src.AccountNumber
                    WHEN MATCHED THEN UPDATE SET UpdatedLUC = src.UpdatedLUC
                    WHEN NOT MATCHED THEN INSERT (CADID, TaxYear, AccountNumber, UpdatedLUC)
                    VALUES (src.CADID, src.TaxYear, src.AccountNumber, src.UpdatedLUC);
                """, row.CADID, row.TaxYear, row.AccountNumber, row.UpdatedLUC)

            elif module == "Landsize Update":
                cursor.execute(f"""
                    MERGE {table_name} AS target
                    USING (SELECT ? AS CADID, ? AS TaxYear, ? AS AccountNumber, ? AS UpdatedLandSize) AS src
                    ON target.CADID = src.CADID AND target.AccountNumber = src.AccountNumber
                    WHEN MATCHED THEN UPDATE SET UpdatedLandSize = src.UpdatedLandSize
                    WHEN NOT MATCHED THEN INSERT (CADID, TaxYear, AccountNumber, UpdatedLandSize)
                    VALUES (src.CADID, src.TaxYear, src.AccountNumber, src.UpdatedLandSize);
                """, row.CADID, row.TaxYear, row.AccountNumber, row.UpdatedLandSize)

            elif module == "GBA Update":
                cursor.execute(f"""
                    MERGE {table_name} AS target
                    USING (SELECT ? AS CADID, ? AS TaxYear, ? AS AccountNumber, ? AS UpdatedGBA) AS src
                    ON target.CADID = src.CADID AND target.AccountNumber = src.AccountNumber
                    WHEN MATCHED THEN UPDATE SET UpdatedGBA = src.UpdatedGBA
                    WHEN NOT MATCHED THEN INSERT (CADID, TaxYear, AccountNumber, UpdatedGBA)
                    VALUES (src.CADID, src.TaxYear, src.AccountNumber, src.UpdatedGBA);
                """, row.CADID, row.TaxYear, row.AccountNumber, row.UpdatedGBA)

            elif module == "Taxroll Insert":
                cursor.execute(f"""
                    MERGE {table_name} AS target
                    USING (SELECT ? AS CADID, ? AS TaxYear, ? AS AccountNumber) AS src
                    ON target.CADID = src.CADID AND target.AccountNumber = src.AccountNumber
                    WHEN NOT MATCHED THEN 
                        INSERT (CADID, TaxYear, AccountNumber)
                        VALUES (src.CADID, src.TaxYear, src.AccountNumber);
                """, row.CADID, row.TaxYear, row.AccountNumber)

        conn.commit()
        conn.close()


# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Taxroll Consolidation Manager", layout="wide")

if "user" not in st.session_state:
    st.session_state["user"] = None
if "updates" not in st.session_state:
    st.session_state["updates"] = {}

# Login page
if not st.session_state["user"]:
    st.title("🔑 Taxroll Consolidation Manager - Login")

    username = st.text_input("Windows Username")
    password = st.text_input("Windows Password", type="password")

    if st.button("Login"):
        if verify_windows_login(username, password):
            st.session_state["user"] = username
            st.success(f"✅ Welcome {st.session_state['user']}")
            st.rerun()
        else:
            st.error("❌ Invalid Windows login")
else:
    # Sidebar Navigation
    st.sidebar.title("📂 Modules")
    page = st.sidebar.radio("Go to", ["🏠 Home", "📊 Value Update", "🏷 LUC Update", "📐 Landsize Update", "🏢 GBA Update",
                                      "📥 Taxroll Insert", "📤 Submit Summary"])

    # Home
    if page == "🏠 Home":
        st.title("Taxroll Consolidation Manager")
        st.info(f"Logged in as: **{st.session_state['user']}**")


    # Reusable Module Loader
    def load_module(module, table_name, required_cols):
        st.title(f"{module}")
        uploaded = st.file_uploader(f"Upload {module} Excel/CSV", type=["xlsx", "csv"], key=module)
        if uploaded:
            if uploaded.name.endswith(".csv"):
                df = pd.read_csv(uploaded)
            else:
                df = pd.read_excel(uploaded)

            # Validate required columns
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                st.error(f"❌ Missing required columns: {missing}")
                return

            st.write("📄 Preview:")
            st.dataframe(df)

            if st.button("Save to DB", key=f"save_{module}"):
                upsert_to_sql(df, table_name, module)
                st.session_state["updates"][module] = df
                st.success(f"✅ {module} saved successfully!")


    # Value Update
    if page == "📊 Value Update":
        load_module("Value Update", "Value_Update_Table", ["CADID", "TaxYear", "AccountNumber", "UpdatedValue"])

    # LUC Update
    elif page == "🏷 LUC Update":
        load_module("LUC Update", "LUC_Update_Table", ["CADID", "TaxYear", "AccountNumber", "UpdatedLUC"])

    # Landsize Update
    elif page == "📐 Landsize Update":
        load_module("Landsize Update", "Landsize_Update_Table",
                    ["CADID", "TaxYear", "AccountNumber", "UpdatedLandSize"])

    # GBA Update
    elif page == "🏢 GBA Update":
        load_module("GBA Update", "GBA_Update_Table", ["CADID", "TaxYear", "AccountNumber", "UpdatedGBA"])

    # Taxroll Insert
    elif page == "📥 Taxroll Insert":
        load_module("Taxroll Insert", "Taxroll_Insert_Table", ["CADID", "TaxYear", "AccountNumber"])

    # Submit Summary
    elif page == "📤 Submit Summary":
        st.title("📤 Final Submission")
        if not st.session_state["updates"]:
            st.warning("No updates recorded yet.")
        else:
            with pd.ExcelWriter("Taxroll_Summary.xlsx") as writer:
                for module, df in st.session_state["updates"].items():
                    df.to_excel(writer, sheet_name=module[:30], index=False)
            st.success("✅ Consolidated Excel report generated: Taxroll_Summary.xlsx")
            st.write(st.session_state["updates"])
