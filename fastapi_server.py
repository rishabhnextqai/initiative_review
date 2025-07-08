#!/usr/bin/env python3
import os
import shutil
import subprocess
import threading
import select
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# ——— Logging —————————————————————————————
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("fastapi_server")

# ——— Dynamic DS root (set at /api/setup) —————————————————
DS_ROOT: str = None

# ——— Env / Secrets —————————————————————————————
DB_HOST     = os.getenv("PG_HOST")
DB_PORT     = int(os.getenv("PG_PORT", "5432"))
DB_USER     = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")
DB_NAME     = os.getenv("DB_NAME")
DB_SCHEMA   = "dwh"

SMTP_SERVER      = os.getenv("SMTP_SERVER")
SMTP_PORT        = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER        = os.getenv("SMTP_USER")
SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",")

RM_API_KEY = os.getenv("RM_API_KEY")

# ——— Security —————————————————————————————
bearer = HTTPBearer()
def verify_token(creds: HTTPAuthorizationCredentials = Depends(bearer)):
    if creds.credentials != RM_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# ——— DB Connection —————————————————————————
DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "dbname": DB_NAME,
}

def get_db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

# ——— LISTEN/NOTIFY Setup ————————————————————
PRIMARY_TABLE   = "f_ranked_initiatives"
PRIMARY_CHANNEL = "table_change_channel"
SECONDARY_TABLE   = "f_genai_extracted_solvedchallenges"
SECONDARY_CHANNEL = "table_change_channel_secondary"

Q_PRIMARY = (
    f"SELECT accountname, initiativename, initiative "
    f"FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
    f"WHERE accountname = %s AND periodid = ("
    f"  SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname = %s"
    f") ORDER BY rank"
)
Q_SECONDARY = (
    f"SELECT DISTINCT product "
    f"FROM {DB_SCHEMA}.{SECONDARY_TABLE} "
    f"WHERE product IS NOT NULL AND product <> 'No Data';"
)

OUT_PRIMARY   = "initiatives.xlsx"
OUT_SECONDARY = "offerings_products.xlsx"

# ——— Operation Scripts (in your repo) —————————————————
BASE_DIR = Path(__file__).parent
OPS = {
    "generate till initiatives": BASE_DIR / "until_initiatives" / "batch_test2_new.py",
    "generate only initiatives": BASE_DIR / "just_initiatives" / "batch_test2_new.py",
    "generate ranking":            BASE_DIR / "initiatives_rank" / "batch_test2_new.py",
    "generate reports":            BASE_DIR / "after_ranking" / "batch_test2_new.py",
}

# ——— Helpers —————————————————————————————
def ensure_trigger(conn, table, channel):
    fn  = f"notify_{table}_change"
    trg = f"watch_{table}_change"
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE OR REPLACE FUNCTION {fn}() RETURNS TRIGGER AS $$
            BEGIN PERFORM pg_notify(%s, ''); RETURN NEW; END;
            $$ LANGUAGE plpgsql;
        """).format(fn=sql.Identifier(fn)), [channel])
        cur.execute(sql.SQL("DROP TRIGGER IF EXISTS {trg} ON {sch}.{tbl};").format(
            trg=sql.Identifier(trg),
            sch=sql.Identifier(DB_SCHEMA),
            tbl=sql.Identifier(table)
        ))
        cur.execute(sql.SQL("""
            CREATE TRIGGER {trg}
            AFTER INSERT OR UPDATE ON {sch}.{tbl}
            FOR EACH ROW EXECUTE PROCEDURE {fn}();
        """).format(
            trg=sql.Identifier(trg),
            sch=sql.Identifier(DB_SCHEMA),
            tbl=sql.Identifier(table),
            fn=sql.Identifier(fn)
        ))
    conn.commit()
    logger.info(f"Trigger ensured on {DB_SCHEMA}.{table}")

def send_email(path):
    msg = MIMEMultipart()
    msg["Subject"] = f"Automated Report: {Path(path).name}"
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(EMAIL_RECIPIENTS)
    msg.attach(MIMEMultipart("alternative"))
    with open(path, "rb") as f:
        part = MIMEApplication(f.read(), Name=Path(path).name)
        part["Content-Disposition"] = f'attachment; filename="{Path(path).name}"'
        msg.attach(part)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASSWORD)
        s.sendmail(SMTP_USER, EMAIL_RECIPIENTS, msg.as_string())
    logger.info(f"Emailed report {path}")

def listen_and_process(table, channel, query, out_file, stop_evt, label):
    while not stop_evt.is_set():
        try:
            conn = get_db_conn()
            ensure_trigger(conn, table, channel)
            cur = conn.cursor()
            cur.execute(sql.SQL("LISTEN {ch};").format(ch=sql.Identifier(channel)))
            logger.info(f"[{label}] Listening on {channel}")
            while not stop_evt.is_set():
                if select.select([conn],[],[],60)==([],[],[]):
                    continue
                conn.poll()
                while conn.notifies:
                    _ = conn.notifies.pop(0)
                    df = pd.read_sql(query, conn, params=(app.state.account, app.state.account))
                    out = Path(DS_ROOT) / out_file
                    df.to_excel(out, index=False)
                    logger.info(f"[{label}] Wrote {out}")
                    send_email(str(out))
            conn.close()
        except Exception as e:
            logger.exception(f"[{label}] Listener error: {e}")
            time.sleep(5)
    logger.info(f"[{label}] Listener stopped")

def update_ranks(rows):
    account = rows[0]["accountname"]
    conn = get_db_conn()
    cur = conn.cursor()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk = f"{DB_SCHEMA}.{PRIMARY_TABLE}_backup_{ts}"
    cur.execute(f"CREATE TABLE {bk} AS SELECT * FROM {DB_SCHEMA}.{PRIMARY_TABLE};")
    cur.execute(
        f"SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname=%s;",
        (account,)
    )
    maxp = cur.fetchone()[0]
    if maxp is None:
        conn.close()
        raise HTTPException(status_code=404, detail="No data")
    cur.execute(
        f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=NULL WHERE accountname=%s AND periodid=%s;",
        (account, maxp)
    )
    for r in rows:
        cur.execute(
            f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=%s "
            f"WHERE accountname=%s AND initiativename=%s AND periodid=%s;",
            (r["rank"], account, r["initiativename"], maxp)
        )
    cur.execute(
        f"DELETE FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
        f"WHERE accountname=%s AND periodid=%s AND rank IS NULL;",
        (account, maxp)
    )
    conn.close()
    return [account]

# ——— FastAPI App —————————————————————————————————
app = FastAPI()

@app.post("/api/setup", dependencies=[Depends(verify_token)])
async def api_setup(
    ds_path:   str = Form(...),
    operation: str = Form(...),
    account:   str = Form(...)
):
    global DS_ROOT
    if "DS" not in ds_path:
        raise HTTPException(status_code=400, detail="`ds_path` must include 'DS'")
    DS_ROOT = ds_path

    # start listeners
    stop_evt = threading.Event()
    threading.Thread(
        target=listen_and_process,
        args=(PRIMARY_TABLE, PRIMARY_CHANNEL, Q_PRIMARY, OUT_PRIMARY, stop_evt, "Primary"),
        daemon=True
    ).start()
    threading.Thread(
        target=listen_and_process,
        args=(SECONDARY_TABLE, SECONDARY_CHANNEL, Q_SECONDARY, OUT_SECONDARY, stop_evt, "Secondary"),
        daemon=True
    ).start()

    # launch operation script
    script = OPS.get(operation)
    if not script:
        raise HTTPException(status_code=400, detail="Invalid operation")
    subprocess.Popen(["python3", str(script)])

    app.state.account   = account
    app.state.stop_evt  = stop_evt

    return {"success": True, "message": "Setup launched"}

@app.post("/api/upload", dependencies=[Depends(verify_token)])
async def api_upload(file: UploadFile = File(...)):
    if DS_ROOT is None:
        raise HTTPException(status_code=400, detail="Call `/api/setup` first")
    outdir = Path(DS_ROOT) / "uploads"
    outdir.mkdir(parents=True, exist_ok=True)
    dest = outdir / file.filename
    with open(dest, "wb") as f:
        f.write(await file.read())
    return {"filename": file.filename}

@app.post("/api/update_ranks", dependencies=[Depends(verify_token)])
async def api_update_ranks(payload: dict):
    if DS_ROOT is None:
        raise HTTPException(status_code=400, detail="Call `/api/setup` first")
    account = payload.get("account")
    rows    = payload.get("rows")
    if not account or not rows:
        raise HTTPException(status_code=400, detail="`account` and `rows` required")
    # inject account
    for r in rows:
        r["accountname"] = account
    updated = update_ranks(rows)
    return {"updated": updated}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_server:app", host="0.0.0.0", port=8000)
