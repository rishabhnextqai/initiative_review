# # #!/usr/bin/env python3
# # import os
# # import requests
# # import streamlit as st

# # # ——— Config from Streamlit Secrets —————————————————
# # API_BASE = os.getenv("RM_API_BASE")  # e.g. https://rm.example.com:8000
# # API_KEY  = os.getenv("RM_API_KEY")
# # HEADERS  = {"Authorization": f"Bearer {API_KEY}"}

# # st.set_page_config(page_title="Initiative Review", layout="wide")
# # st.title("Initiative Review (Cloud UI)")

# # page = st.sidebar.selectbox("Go to", ["Setup", "Upload Feedback", "Upload Ranks"])

# # if page == "Setup":
# #     st.header("1. Setup & Operations")
# #     ds_path   = st.text_input("Full path including 'DS' folder")
# #     account   = st.text_input("Account Name")
# #     operation = st.selectbox("Select Operation", [
# #         "generate till initiatives",
# #         "generate only initiatives",
# #         "generate ranking",
# #         "generate reports",
# #     ])

# #     if st.button("Run Setup"):
# #         if not ds_path or not account:
# #             st.error("Both DS Path and Account are required.")
# #         else:
# #             r = requests.post(
# #                 f"{API_BASE}/api/setup",
# #                 headers=HEADERS,
# #                 data={
# #                     "ds_path": ds_path,
# #                     "operation": operation,
# #                     "account": account
# #                 },
# #                 timeout=30
# #             )
# #             try:
# #                 st.json(r.json())
# #             except:
# #                 st.error(f"Setup failed ({r.status_code}): {r.text}")

# # elif page == "Upload Feedback":
# #     st.header("2. Upload Feedback")
# #     uploaded = st.file_uploader("Choose a file")
# #     if uploaded:
# #         files = {"file": (uploaded.name, uploaded.getvalue())}
# #         r = requests.post(
# #             f"{API_BASE}/api/upload",
# #             headers=HEADERS,
# #             files=files,
# #             timeout=30
# #         )
# #         if r.status_code == 200:
# #             st.success(f"Uploaded: {r.json().get('filename')}")
# #         else:
# #             st.error(f"Upload failed ({r.status_code}): {r.text}")

# # elif page == "Upload Ranks":
# #     st.header("3. Upload Ranks")
# #     account = st.text_input("Account Name for Ranks")
# #     if "rows" not in st.session_state:
# #         st.session_state.rows = []

# #     if st.button("➕ Add Row"):
# #         st.session_state.rows.append({"initiativename": "", "rank": 1})

# #     for idx, row in enumerate(st.session_state.rows):
# #         c1, c2, c3 = st.columns([4, 1, 1])
# #         ini = c1.text_input("Initiative", value=row["initiativename"], key=f"ini{idx}")
# #         rk = c2.number_input("Rank", value=row["rank"], min_value=1, key=f"rk{idx}")
# #         if c3.button("❌", key=f"del{idx}"):
# #             st.session_state.rows.pop(idx)
# #             st.experimental_rerun()
# #         st.session_state.rows[idx] = {"initiativename": ini, "rank": rk}

# #     if st.button("Submit Ranks"):
# #         if not account:
# #             st.error("Account Name is required.")
# #         elif not st.session_state.rows:
# #             st.warning("No rows to submit.")
# #         else:
# #             payload = {"account": account, "rows": st.session_state.rows}
# #             r = requests.post(
# #                 f"{API_BASE}/api/update_ranks",
# #                 headers=HEADERS,
# #                 json=payload,
# #                 timeout=30
# #             )
# #             if r.status_code == 200:
# #                 st.success(f"Updated: {r.json().get('updated')}")
# #                 st.session_state.rows = []
# #             else:
# #                 st.error(f"Update failed ({r.status_code}): {r.text}")


# #!/usr/bin/env python3
# import os
# import sys
# import shutil
# import threading
# import select
# import time
# import logging
# from datetime import datetime
# from pathlib import Path

# from dotenv import load_dotenv
# load_dotenv()

# import pandas as pd
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# import smtplib
# from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from email.mime.multipart import MIMIMultipart
# from email.mime.application import MIMEApplication
# from email.mime.text import MIMEText

# # ─── Logging ─────────────────────────────────────────────────
# logging.basicConfig(
#     format="%(asctime)s %(levelname)s %(message)s",
#     level=logging.INFO
# )
# logger = logging.getLogger("fastapi_server")

# # ─── Globals set at /api/setup ───────────────────────────────
# DS_ROOT: Path = None
# DB_NAME: str  = None

# # ─── Static secrets from .env ─────────────────────────────────
# DB_HOST     = os.getenv("PG_HOST")
# DB_PORT     = int(os.getenv("PG_PORT", "5432"))
# DB_USER     = os.getenv("PG_USER")
# DB_PASSWORD = os.getenv("PG_PASSWORD")

# SMTP_SERVER      = os.getenv("SMTP_SERVER")
# SMTP_PORT        = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER        = os.getenv("SMTP_USER")
# SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD")
# EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",")

# RM_API_KEY = os.getenv("RM_API_KEY")
# if not RM_API_KEY:
#     logger.error("RM_API_KEY must be set in .env")
#     sys.exit(1)

# # ─── FastAPI + security ───────────────────────────────────────
# app = FastAPI()
# bearer = HTTPBearer()
# def verify_token(creds: HTTPAuthorizationCredentials = Depends(bearer)):
#     if creds.credentials != RM_API_KEY:
#         raise HTTPException(403, "Invalid API key")

# # ─── Database connection builder ──────────────────────────────
# DB_SCHEMA = "dwh"
# def get_db_conn():
#     if not DB_NAME:
#         raise HTTPException(400, "DB_NAME not set; call /api/setup first")
#     conn = psycopg2.connect(
#         host=DB_HOST, port=DB_PORT,
#         user=DB_USER, password=DB_PASSWORD,
#         dbname=DB_NAME
#     )
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#     return conn

# # ─── LISTEN/NOTIFY configuration ──────────────────────────────
# PRIMARY_TABLE   = "f_ranked_initiatives"
# PRIMARY_CHANNEL = "table_change_channel"
# SECONDARY_TABLE   = "f_genai_extracted_solvedchallenges"
# SECONDARY_CHANNEL = "table_change_channel_secondary"

# Q_PRIMARY = (
#     f"SELECT accountname, initiativename, initiative "
#     f"FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
#     f"WHERE accountname = %s AND periodid = ("
#     f"  SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname = %s"
#     f") ORDER BY rank"
# )
# Q_SECONDARY = (
#     f"SELECT DISTINCT product "
#     f"FROM {DB_SCHEMA}.{SECONDARY_TABLE} "
#     f"WHERE product IS NOT NULL AND product <> 'No Data';"
# )

# OUT_PRIMARY   = "initiatives.xlsx"
# OUT_SECONDARY = "offerings_products.xlsx"

# # ─── Repo operation folders ────────────────────────────────────
# REPO_BASE   = Path(__file__).parent
# OP_FOLDERS = {
#     "generate till initiatives":    "until_initiatives",
#     "generate only initiatives":    "just_initiatives",
#     "generate ranking":             "initiatives_rank",
#     "generate reports":             "after_ranking",
# }

# # ─── Helpers ─────────────────────────────────────────────────
# def ensure_trigger(conn, table, channel):
#     fn  = f"notify_{table}_change"
#     trg = f"watch_{table}_change"
#     with conn.cursor() as cur:
#         cur.execute(sql.SQL("""
#             CREATE OR REPLACE FUNCTION {fn}() RETURNS TRIGGER AS $$
#             BEGIN PERFORM pg_notify(%s, ''); RETURN NEW; END;
#             $$ LANGUAGE plpgsql;
#         """).format(fn=sql.Identifier(fn)), [channel])
#         cur.execute(sql.SQL("DROP TRIGGER IF EXISTS {trg} ON {sch}.{tbl};").format(
#             trg=sql.Identifier(trg),
#             sch=sql.Identifier(DB_SCHEMA),
#             tbl=sql.Identifier(table)
#         ))
#         cur.execute(sql.SQL("""
#             CREATE TRIGGER {trg}
#             AFTER INSERT OR UPDATE ON {sch}.{tbl}
#             FOR EACH ROW EXECUTE PROCEDURE {fn}();
#         """).format(
#             trg=sql.Identifier(trg),
#             sch=sql.Identifier(DB_SCHEMA),
#             tbl=sql.Identifier(table),
#             fn=sql.Identifier(fn)
#         ))
#     conn.commit()

# def send_email(path: Path):
#     msg = MIMIMultipart()
#     msg["Subject"] = f"Automated Report: {path.name}"
#     msg["From"]    = SMTP_USER
#     msg["To"]      = ", ".join(EMAIL_RECIPIENTS)
#     msg.attach(MIMIMultipart("alternative"))
#     with open(path, "rb") as f:
#         part = MIMEApplication(f.read(), Name=path.name)
#         part["Content-Disposition"] = f'attachment; filename="{path.name}"'
#         msg.attach(part)
#     with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
#         s.starttls()
#         s.login(SMTP_USER, SMTP_PASSWORD)
#         s.sendmail(SMTP_USER, EMAIL_RECIPIENTS, msg.as_string())

# def listen_and_process(table, channel, query, out_file, stop_evt, label):
#     while not stop_evt.is_set():
#         try:
#             conn = get_db_conn()
#             ensure_trigger(conn, table, channel)
#             cur = conn.cursor()
#             cur.execute(sql.SQL("LISTEN {ch};").format(ch=sql.Identifier(channel)))
#             while not stop_evt.is_set():
#                 if select.select([conn], [], [], 60) == ([], [], []):
#                     continue
#                 conn.poll()
#                 while conn.notifies:
#                     conn.notifies.pop(0)
#                     df = pd.read_sql(query, conn,
#                                      params=(app.state.account, app.state.account))
#                     outp = DS_ROOT / out_file
#                     df.to_excel(outp, index=False)
#                     send_email(outp)
#             conn.close()
#         except Exception as e:
#             logger.exception(f"{label} listener error: {e}")
#             time.sleep(5)

# def update_ranks(rows):
#     acct = rows[0]["accountname"]
#     conn = get_db_conn(); cur = conn.cursor()
#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     backup = f"{DB_SCHEMA}.{PRIMARY_TABLE}_backup_{ts}"
#     cur.execute(f"CREATE TABLE {backup} AS SELECT * FROM {DB_SCHEMA}.{PRIMARY_TABLE};")
#     cur.execute(
#         f"SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname=%s;",
#         (acct,)
#     )
#     maxp = cur.fetchone()[0]
#     if maxp is None:
#         conn.close()
#         raise HTTPException(404, "No data for account")
#     cur.execute(
#         f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=NULL "
#         f"WHERE accountname=%s AND periodid=%s;",
#         (acct, maxp)
#     )
#     for r in rows:
#         cur.execute(
#             f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=%s "
#             f"WHERE accountname=%s AND initiativename=%s AND periodid=%s;",
#             (r["rank"], acct, r["initiativename"], maxp)
#         )
#     cur.execute(
#         f"DELETE FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
#         f"WHERE accountname=%s AND periodid=%s AND rank IS NULL;",
#         (acct, maxp)
#     )
#     conn.close()
#     return [acct]

# # ─── Endpoints ────────────────────────────────────────────────
# @app.post("/api/setup", dependencies=[Depends(verify_token)])
# async def api_setup(
#     ds_path:   str = Form(...),
#     operation: str = Form(...),
#     account:   str = Form(...)
# ):
#     global DS_ROOT, DB_NAME
#     # Normalize and locate 'DS'
#     norm = Path(ds_path).expanduser()
#     parts = norm.parts
#     if "DS" not in parts:
#         raise HTTPException(400, "`ds_path` must include a 'DS' folder")
#     ds_idx  = parts.index("DS")
#     DS_ROOT = Path(*parts[: ds_idx+1])
#     logger.info(f"DS_ROOT set to: {DS_ROOT}")

#     # Derive DB_NAME
#     DB_NAME = next((p for p in parts if p.startswith("qpilot_v")), parts[-1])
#     logger.info(f"DB_NAME derived as: {DB_NAME}")

#     # Copy single script
#     folder_key  = OP_FOLDERS.get(operation)
#     if not folder_key:
#         raise HTTPException(400, "Invalid operation")
#     src_script  = REPO_BASE / folder_key / "batch_test2_new.py"
#     if not src_script.is_file():
#         raise HTTPException(500, f"Source script missing at {src_script}")
#     dest_script = DS_ROOT / src_script.name
#     logger.info(f"Copying script {src_script} → {dest_script}")
#     shutil.copy2(src_script, dest_script)
#     logger.info("Script copy complete")

#     # Launch listeners once
#     if not getattr(app.state, "listeners_started", False):
#         stop_evt = threading.Event()
#         threading.Thread(
#             target=listen_and_process,
#             args=(PRIMARY_TABLE, PRIMARY_CHANNEL, Q_PRIMARY, OUT_PRIMARY, stop_evt, "Primary"),
#             daemon=True
#         ).start()
#         threading.Thread(
#             target=listen_and_process,
#             args=(SECONDARY_TABLE, SECONDARY_CHANNEL, Q_SECONDARY, OUT_SECONDARY, stop_evt, "Secondary"),
#             daemon=True
#         ).start()
#         app.state.listeners_started = True
#         logger.info("Listeners launched")

#     # Store account
#     app.state.account = account

#     return {"success": True, "message": f"'{src_script.name}' copied to DS_ROOT"}


#!/usr/bin/env python3
import os
import requests
import streamlit as st

# ─── Config from Streamlit Secrets ───────────────────────────
API_BASE = os.getenv("RM_API_BASE")  # e.g. https://rm.example.com:8000
API_KEY  = os.getenv("RM_API_KEY")
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}

st.set_page_config(page_title="Initiative Review", layout="wide")
st.title("Initiative Review (Cloud UI)")

page = st.sidebar.selectbox("Go to", ["Setup", "Upload Feedback", "Upload Ranks"])

if page == "Setup":
    st.header("1. Setup & Operations")
    ds_path   = st.text_input("Full path including 'DS' folder")
    operation = st.selectbox("Select Operation", [
        "generate till initiatives",
        "generate only initiatives",
        "generate ranking",
        "generate reports",
    ])
    account   = st.text_input("Account Name")

    if st.button("Run Setup"):
        if not ds_path or not operation or not account:
            st.error("All fields are required.")
        else:
            r = requests.post(
                f"{API_BASE}/api/setup",
                headers=HEADERS,
                data={"ds_path": ds_path, "operation": operation, "account": account},
                timeout=30
            )
            if r.status_code == 200:
                st.success(r.json())
            else:
                st.error(f"Error ({r.status_code}): {r.text}")

elif page == "Upload Feedback":
    st.header("2. Upload Feedback")
    uploaded = st.file_uploader("Choose a file to upload")
    if uploaded:
        files = {"file": (uploaded.name, uploaded.getvalue())}
        r = requests.post(
            f"{API_BASE}/api/upload",
            headers=HEADERS,
            files=files,
            timeout=30
        )
        if r.status_code == 200:
            st.success(f"Uploaded: {r.json().get('filename')}")
        else:
            st.error(f"Upload failed ({r.status_code}): {r.text}")

elif page == "Upload Ranks":
    st.header("3. Upload Ranks")
    account = st.text_input("Account Name")

    if "rows" not in st.session_state:
        st.session_state.rows = []

    if st.button("➕ Add Row"):
        st.session_state.rows.append({"initiativename": "", "rank": 1})

    for idx, row in enumerate(st.session_state.rows):
        c1, c2, c3 = st.columns([4, 1, 1])
        ini = c1.text_input("Initiative", value=row["initiativename"], key=f"ini{idx}")
        rk  = c2.number_input("Rank", value=row["rank"], min_value=1, key=f"rk{idx}")
        if c3.button("❌", key=f"del{idx}"):
            st.session_state.rows.pop(idx)
            st.experimental_rerun()
        st.session_state.rows[idx] = {"initiativename": ini, "rank": rk}

    if st.button("Submit Ranks"):
        if not account:
            st.error("Account Name is required.")
        elif not st.session_state.rows:
            st.warning("No rows to submit.")
        else:
            payload = {"account": account, "rows": st.session_state.rows}
            r = requests.post(
                f"{API_BASE}/api/update_ranks",
                headers=HEADERS,
                json=payload,
                timeout=30
            )
            if r.status_code == 200:
                st.success(f"Updated: {r.json().get('updated')}")
                st.session_state.rows = []
            else:
                st.error(f"Update failed ({r.status_code}): {r.text}")
