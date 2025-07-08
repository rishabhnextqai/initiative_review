#!/usr/bin/env python3
import os
import requests
import streamlit as st

# ——— Config from Streamlit Secrets —————————————————
API_BASE = os.getenv("RM_API_BASE")  # e.g. https://rm.example.com:8000
API_KEY  = os.getenv("RM_API_KEY")
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}

st.set_page_config(page_title="Initiative Review", layout="wide")
st.title("Initiative Review (Cloud UI)")

page = st.sidebar.selectbox("Go to", ["Setup", "Upload Feedback", "Upload Ranks"])

if page == "Setup":
    st.header("1. Setup & Operations")
    ds_path   = st.text_input("Full path including 'DS' folder")
    account   = st.text_input("Account Name")
    operation = st.selectbox("Select Operation", [
        "generate till initiatives",
        "generate only initiatives",
        "generate ranking",
        "generate reports",
    ])

    if st.button("Run Setup"):
        if not ds_path or not account:
            st.error("Both DS Path and Account are required.")
        else:
            r = requests.post(
                f"{API_BASE}/api/setup",
                headers=HEADERS,
                data={
                    "ds_path": ds_path,
                    "operation": operation,
                    "account": account
                },
                timeout=30
            )
            try:
                st.json(r.json())
            except:
                st.error(f"Setup failed ({r.status_code}): {r.text}")

elif page == "Upload Feedback":
    st.header("2. Upload Feedback")
    uploaded = st.file_uploader("Choose a file")
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
    account = st.text_input("Account Name for Ranks")
    if "rows" not in st.session_state:
        st.session_state.rows = []

    if st.button("➕ Add Row"):
        st.session_state.rows.append({"initiativename": "", "rank": 1})

    for idx, row in enumerate(st.session_state.rows):
        c1, c2, c3 = st.columns([4, 1, 1])
        ini = c1.text_input("Initiative", value=row["initiativename"], key=f"ini{idx}")
        rk = c2.number_input("Rank", value=row["rank"], min_value=1, key=f"rk{idx}")
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
