#!/usr/bin/env python3
import os
import subprocess
import threading
import logging
import select
import time
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
logger = logging.getLogger("initiative_review")

# -----------------------------
# PostgreSQL / Listener Configuration (from GitHub Secrets)
# -----------------------------
PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = int(os.getenv("PG_PORT", 5432))
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
DB_SCHEMA   = "dwh"

DB_CONFIG = {
    "host": PG_HOST,
    "port": PG_PORT,
    "user": PG_USER,
    "password": PG_PASSWORD,
    "dbname": None,
}

PRIMARY_LISTEN_TABLE   = "f_ranked_initiatives"
PRIMARY_LISTEN_CHANNEL = "table_change_channel"
SECONDARY_LISTEN_TABLE   = "f_genai_extracted_solvedchallenges"
SECONDARY_LISTEN_CHANNEL = "table_change_channel_secondary"

CHANGE_QUERY_TEMPLATE_PRIMARY = (
    f"SELECT accountname, initiativename, initiative "
    f"FROM {DB_SCHEMA}.{PRIMARY_LISTEN_TABLE} "
    f"WHERE accountname = %s AND periodid = ("
    f"  SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_LISTEN_TABLE} WHERE accountname = %s"
    f") ORDER BY rank"
)
CHANGE_QUERY_TEMPLATE_SECONDARY = (
    f"SELECT DISTINCT product "
    f"FROM {DB_SCHEMA}.{SECONDARY_LISTEN_TABLE} "
    f"WHERE product IS NOT NULL AND product <> 'No Data';"
)

OUTPUT_FILE_PRIMARY   = "initiatives.xlsx"
OUTPUT_FILE_SECONDARY = "offerings_products.xlsx"

# -----------------------------
# Email Configuration (from GitHub Secrets)
# -----------------------------
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", 587))
SMTP_USER     = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_TO      = os.getenv("EMAIL_RECIPIENTS", "").split(",")

# -----------------------------
# Operation → Relative Path Mapping
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
OPERATION_CONFIG = {
    "generate till initiatives": BASE_DIR / "until_initiatives"      / "batch_test2_new.py",
    "generate only initiatives":   BASE_DIR / "just_initiatives"      / "batch_test2_new.py",
    "generate ranking":            BASE_DIR / "initiatives_rank"      / "batch_test2_new.py",
    "generate reports":            BASE_DIR / "after_ranking"         / "batch_test2_new.py",
}

# -----------------------------
# Session State Defaults
# -----------------------------
defaults = {
    "DS_ROOT_DIR":       "",
    "UPLOAD_DIR":        "",
    "ACCOUNT_NAME":      "",
    "DB_NAME":           "",
    "setup_done":        False,
    "listeners_started": False,
    "rank_rows":         []
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# -----------------------------
# Utility Functions
# -----------------------------
def init_db(db_name: str):
    DB_CONFIG["dbname"] = db_name
    st.session_state["DB_NAME"] = db_name
    logger.info(f"Configured DB: {db_name}")

def get_db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def ensure_trigger(conn, table: str, channel: str):
    fn  = f"notify_{table}_change"
    trg = f"watch_{table}_change"
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                CREATE OR REPLACE FUNCTION {fn}() RETURNS TRIGGER AS $$
                BEGIN
                    PERFORM pg_notify(%s, '');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """).format(fn=sql.Identifier(fn)),
            [channel],
        )
        cur.execute(
            sql.SQL("DROP TRIGGER IF EXISTS {trg} ON {schema}.{tbl};").format(
                trg=sql.Identifier(trg),
                schema=sql.Identifier(DB_SCHEMA),
                tbl=sql.Identifier(table),
            )
        )
        cur.execute(
            sql.SQL("""
                CREATE TRIGGER {trg}
                AFTER INSERT OR UPDATE ON {schema}.{tbl}
                FOR EACH ROW EXECUTE PROCEDURE {fn}();
            """).format(
                trg=sql.Identifier(trg),
                schema=sql.Identifier(DB_SCHEMA),
                tbl=sql.Identifier(table),
                fn=sql.Identifier(fn),
            )
        )
    conn.commit()
    logger.info(f"Trigger ensured on {DB_SCHEMA}.{table}")

def send_email(attachment_path: str):
    msg = MIMEMultipart()
    msg["Subject"] = f"Automated Report: {Path(attachment_path).name}"
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(EMAIL_TO)
    msg.attach(MIMEText("Please find attached the latest report.", "plain"))
    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=Path(attachment_path).name)
        part["Content-Disposition"] = f'attachment; filename="{Path(attachment_path).name}"'
        msg.attach(part)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())
    logger.info(f"Email sent with {attachment_path}")

def listen_and_process(table, channel, query, output_file, stop_event, label):
    while not stop_event.is_set():
        try:
            conn = get_db_conn()
            ensure_trigger(conn, table, channel)
            cur = conn.cursor()
            cur.execute(sql.SQL("LISTEN {ch};").format(ch=sql.Identifier(channel)))
            logger.info(f"[{label}] Listening on '{channel}'")
            while not stop_event.is_set():
                if select.select([conn], [], [], 60) == ([], [], []):
                    continue
                conn.poll()
                while conn.notifies:
                    n = conn.notifies.pop(0)
                    logger.info(f"[{label}] Notification on {n.channel}")
                    df = pd.read_sql(query, conn, params=(st.session_state["ACCOUNT_NAME"],)*2)
                    df.to_excel(output_file, index=False)
                    logger.info(f"[{label}] Wrote {output_file}")
                    send_email(output_file)
            conn.close()
        except Exception as e:
            logger.exception(f"[{label}] Listener error: {e}")
            time.sleep(5)
    logger.info(f"[{label}] Listener stopped")

def update_ranks_sync(items: list[dict]) -> list[str]:
    account = items[0]["accountname"]
    conn    = get_db_conn()
    cur     = conn.cursor()
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup  = f"{DB_SCHEMA}.f_ranked_initiatives_backup_{ts}"
    cur.execute(f"CREATE TABLE {backup} AS SELECT * FROM {DB_SCHEMA}.f_ranked_initiatives;")
    cur.execute(
        "SELECT MAX(periodid) FROM dwh.f_ranked_initiatives WHERE accountname = %s;",
        (account,)
    )
    maxp = cur.fetchone()[0]
    if maxp is None:
        conn.close()
        raise ValueError(f"No data for '{account}'")
    cur.execute(
        "UPDATE dwh.f_ranked_initiatives SET rank = NULL WHERE accountname = %s AND periodid = %s;",
        (account, maxp)
    )
    for row in items:
        cur.execute(
            "UPDATE dwh.f_ranked_initiatives SET rank = %s WHERE accountname = %s AND initiativename = %s AND periodid = %s;",
            (row["rank"], account, row["initiativename"], maxp)
        )
    cur.execute(
        "DELETE FROM dwh.f_ranked_initiatives WHERE accountname = %s AND periodid = %s AND rank IS NULL;",
        (account, maxp)
    )
    conn.close()
    return [account]

# -----------------------------
# Streamlit App Layout
# -----------------------------
st.set_page_config(page_title="Initiative Review", layout="wide")
st.title("Initiative Review")

page = st.sidebar.selectbox("Go to", [
    "Setup & Operations",
    "Upload Feedback",
    "Upload Ranks"
])

# ─── Setup & Operations ──────────────────────────────────────────────────────
if page == "Setup & Operations":
    st.header("1. Setup & Operations")
    ds_path   = st.text_input("Full path including ‘DS’ folder", st.session_state["DS_ROOT_DIR"])
    acct      = st.text_input("Account Name",                  st.session_state["ACCOUNT_NAME"])
    operation = st.selectbox("Select Operation",              list(OPERATION_CONFIG.keys()))

    if st.button("Run Setup & Operation"):
        try:
            norm   = os.path.normpath(ds_path.strip())
            parts  = norm.split(os.sep)
            if "DS" not in parts:
                raise ValueError("Path must include a ‘DS’ folder")
            idx    = parts.index("DS")
            drive, _ = os.path.splitdrive(norm)
            base   = (drive + os.sep) if drive else os.sep
            ds_root = os.path.join(base, *parts[: idx+1])
            os.makedirs(ds_root, exist_ok=True)
            os.makedirs(norm, exist_ok=True)

            db = next((p for p in parts if p.startswith("qpilot_v")), parts[-1])
            init_db(db)

            # launch operation script
            script_path = OPERATION_CONFIG[operation]
            start_operation_thread = threading.Thread(target=lambda: subprocess.run(
                ["python3", str(script_path)], check=True
            ), daemon=True)
            start_operation_thread.start()

            # start listeners once
            if not st.session_state["listeners_started"]:
                st.session_state["stop_event_primary"]   = threading.Event()
                st.session_state["stop_event_secondary"] = threading.Event()
                threading.Thread(
                    target=listen_and_process,
                    args=(
                        PRIMARY_LISTEN_TABLE,
                        PRIMARY_LISTEN_CHANNEL,
                        CHANGE_QUERY_TEMPLATE_PRIMARY,
                        OUTPUT_FILE_PRIMARY,
                        st.session_state["stop_event_primary"],
                        "Primary",
                    ),
                    daemon=True
                ).start()
                threading.Thread(
                    target=listen_and_process,
                    args=(
                        SECONDARY_LISTEN_TABLE,
                        SECONDARY_LISTEN_CHANNEL,
                        CHANGE_QUERY_TEMPLATE_SECONDARY,
                        OUTPUT_FILE_SECONDARY,
                        st.session_state["stop_event_secondary"],
                        "Secondary",
                    ),
                    daemon=True
                ).start()
                st.session_state["listeners_started"] = True

            st.session_state.update({
                "DS_ROOT_DIR":      ds_root,
                "UPLOAD_DIR":       norm,
                "ACCOUNT_NAME":     acct.strip(),
                "setup_done":       True,
                "rank_rows":        []
            })

            st.success(
                "✅ Setup complete:\n"
                f"- DS root: `{ds_root}`\n"
                f"- Upload dir: `{norm}`\n"
                f"- Account: `{acct}`\n"
                f"- DB: `{db}`\n"
                f"- Operation: `{operation}` started.\n"
                f"- Listeners running."
            )
        except Exception as e:
            st.exception(f"Setup failed: {e}")

# ─── Upload Feedback ─────────────────────────────────────────────────────────
elif page == "Upload Feedback":
    st.header("2. Upload Feedback")
    if not st.session_state["setup_done"]:
        st.warning("⚠️ Please complete **Setup & Operations** first.")
    uploaded = st.file_uploader("Choose a file to upload", accept_multiple_files=False)
    if uploaded:
        if not st.session_state["setup_done"]:
            st.error("Upload directory not set. Complete Setup & Operations first.")
        else:
            fn = Path(uploaded.name).name
            if ".." in fn or fn.startswith(("/", "\\")):
                st.error("Invalid filename")
            else:
                dest = Path(st.session_state["UPLOAD_DIR"]) / fn
                with open(dest, "wb") as f:
                    f.write(uploaded.getbuffer())
                st.success(f"Saved `{fn}` to `{st.session_state['UPLOAD_DIR']}`")

# ─── Upload Ranks ────────────────────────────────────────────────────────────
elif page == "Upload Ranks":
    st.header("3. Upload Ranks")
    if not st.session_state["setup_done"]:
        st.warning("⚠️ Please complete **Setup & Operations** first.")

    acct_r = st.text_input("Account Name for Ranks", value=st.session_state["ACCOUNT_NAME"])

    if st.button("➕ Add Row"):
        st.session_state["rank_rows"].append({"initiativename": "", "rank": 1})

    for i, row in enumerate(st.session_state["rank_rows"]):
        c1, c2, c3 = st.columns([4,1,1])
        ini = c1.text_input("Initiative", value=row["initiativename"], key=f"ini_{i}")
        rk  = c2.number_input("Rank", min_value=1, value=row["rank"], step=1, key=f"rk_{i}")
        if c3.button("❌", key=f"del_{i}"):
            st.session_state["rank_rows"].pop(i)
            st.experimental_rerun()
        st.session_state["rank_rows"][i] = {"initiativename": ini, "rank": rk}

    if st.button("Submit Ranks"):
        if not st.session_state["setup_done"]:
            st.error("Cannot update ranks until Setup & Operations is done.")
        else:
            try:
                items = []
                for r in st.session_state["rank_rows"]:
                    if r["initiativename"] and r["rank"]:
                        items.append({
                            "accountname": acct_r.strip(),
                            "initiativename": r["initiativename"].strip(),
                            "rank": int(r["rank"]),
                        })
                if not items:
                    st.warning("No valid rows to submit.")
                else:
                    updated = update_ranks_sync(items)
                    st.success(f"Ranks updated for: {', '.join(updated)}")
                    st.session_state["rank_rows"] = []
            except Exception as e:
                st.exception(f"Failed to update ranks: {e}")
