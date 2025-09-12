# app.py
# OpperWorks Stock Take — robust CRUD inventory editor
# v2.0  (stable row-id, precise merge, delete, reorder, reliable save)

import os
import io
import uuid
from datetime import datetime
from typing import List, Optional

import pandas as pd
import streamlit as st

# --------------------------- CONFIG ---------------------------

APP_TITLE = "OpperWorks Stock Take"
DEFAULT_DATA_PATH = "data/inventory.csv"
BACKUP_DIR = "backups"
SNAPSHOT_DIR = "snapshots"

# Known logo locations (first existing will be used)
OPPERWORKS_LOGO_CANDIDATES: List[str] = [
    "/mnt/data/OpperWorks Logo.png",  # container-provided
    "assets/logo_OpperWorks.png",
    "assets/OpperWorks Logo.png",
]
PG_BISON_LOGO_CANDIDATES: List[str] = [
    "/mnt/data/PG Bison.jpg",         # container-provided
    "assets/logo_PG_Bison.jpg",
    "assets/PG Bison.jpg",
]

# Inventory schema (user-visible columns)
USER_COLUMNS = [
    "Item ID",            # optional (kept if present)
    "Item Code",
    "Description",
    "Category",
    "UOM",
    "Location",
    "Qty On Hand",
    "Min Level",
    "Max Level",
    "Unit Cost (ZAR)",
    "Last Updated",
]
# Hidden stable row key used for all merges/updates
ROW_ID_COL = "_row_id"
# Delete flag column (visible)
DELETE_COL = "Delete?"

INT_COLS = ["Qty On Hand", "Min Level", "Max Level"]
FLOAT_COLS = ["Unit Cost (ZAR)"]
STRING_COLS = ["Item ID", "Item Code", "Description", "Category", "UOM", "Location", "Last Updated"]

DEFAULT_SORT = ["Category", "Item Code"]  # can be changed in UI

# ------------------------ UTILITIES ---------------------------

def ensure_dirs():
    os.makedirs(os.path.dirname(DEFAULT_DATA_PATH) or ".", exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)

def find_first_existing(paths: List[str]) -> Optional[str]:
    for p in paths:
        if os.path.exists(p):
            return p
    return None

def ts_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def coerce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required columns exist and dtypes are sane. Keep any extra columns present in file."""
    df = df.copy()

    # Hidden row id
    if ROW_ID_COL not in df.columns:
        df[ROW_ID_COL] = None

    # Visible columns: create if missing
    for c in USER_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # String cleanup
    for c in STRING_COLS:
        df[c] = df[c].astype("string").fillna("").str.strip()

    # Numerics
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype("float64")

    # Delete flag for editor convenience (not persisted)
    df[DELETE_COL] = False

    # Make sure row ids exist and are unique
    mask_missing = df[ROW_ID_COL].isna() | (df[ROW_ID_COL].astype(str).str.len() == 0)
    if mask_missing.any():
        existing = set(df.loc[~mask_missing, ROW_ID_COL].astype(str))
        for idx in df.index[mask_missing]:
            rid = f"opw_{uuid.uuid4().hex}"
            while rid in existing:
                rid = f"opw_{uuid.uuid4().hex}"
            df.at[idx, ROW_ID_COL] = rid
            existing.add(rid)

    # Column order: keep extras but place our columns first in a consistent order
    leading = [ROW_ID_COL] + USER_COLUMNS + [DELETE_COL]
    extras = [c for c in df.columns if c not in leading]
    df = df[leading + extras]

    return df

def init_empty_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=[ROW_ID_COL] + USER_COLUMNS)
    return coerce_schema(df)

def read_any(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return init_empty_df()
    if path.lower().endswith(".xlsx"):
        return pd.read_excel(path)
    return pd.read_csv(path)

def write_any(df: pd.DataFrame, path: str):
    # Don't persist UI-only columns
    out = df.drop(columns=[DELETE_COL], errors="ignore").copy()
    # Ensure schema before persist
    out = coerce_schema(out)
    # Update "Last Updated" only for rows that changed during this save cycle is complex;
    # simple+reliable: set to now for all rows that are part of the edited set. Here we set for all.
    out["Last Updated"] = ts_now()

    # Make a CSV backup of the current file (if exists), regardless of primary format
    if os.path.exists(path):
        stem = os.path.splitext(os.path.basename(path))[0]
        bkp = os.path.join(BACKUP_DIR, f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            prev = read_any(path)
            prev.to_csv(bkp, index=False)
        except Exception:
            pass  # continue save anyway

    # Persist
    if path.lower().endswith(".xlsx"):
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            out.to_excel(writer, index=False, sheet_name="Inventory")
    else:
        out.to_csv(path, index=False)

def apply_sort(df: pd.DataFrame, sort_cols: List[str], ascending: bool = True) -> pd.DataFrame:
    keep_cols = [c for c in sort_cols if c in df.columns]
    if keep_cols:
        return df.sort_values(keep_cols, ascending=ascending, kind="mergesort").reset_index(drop=True)
    return df.reset_index(drop=True)

def export_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df_out = df.drop(columns=[DELETE_COL], errors="ignore").copy()
        df_out.to_excel(writer, index=False, sheet_name="Inventory")
        wb = writer.book
        money_fmt = wb.add_format({"num_format": "#,##0.00"})
        qty_fmt = wb.add_format({"num_format": "0"})
        ws = writer.sheets["Inventory"]
        cols = {c: i for i, c in enumerate(df_out.columns)}
        if "Unit Cost (ZAR)" in cols:
            ws.set_column(cols["Unit Cost (ZAR)"], cols["Unit Cost (ZAR)"], 14, money_fmt)
        for c in INT_COLS:
            if c in cols:
                ws.set_column(cols[c], cols[c], 10, qty_fmt)
    out.seek(0)
    return out.read()

# ------------------------- STATE ------------------------------

st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")
ensure_dirs()

if "data_path" not in st.session_state:
    st.session_state.data_path = DEFAULT_DATA_PATH
if "df" not in st.session_state:
    st.session_state.df = coerce_schema(read_any(st.session_state.data_path))
if "sort_cols" not in st.session_state:
    st.session_state.sort_cols = DEFAULT_SORT
if "sort_asc" not in st.session_state:
    st.session_state.sort_asc = True
if "autosave" not in st.session_state:
    st.session_state.autosave = False
if "dirty" not in st.session_state:
    st.session_state.dirty = False

# -------------------------- SIDEBAR ---------------------------

with st.sidebar:
    st.markdown("### Branding")
    brand = st.selectbox("Select brand logo", ["OpperWorks", "PG Bison", "None"], index=0)
    logo_path = None
    if brand == "OpperWorks":
        logo_path = find_first_existing(OPPERWORKS_LOGO_CANDIDATES)
    elif brand == "PG Bison":
        logo_path = find_first_existing(PG_BISON_LOGO_CANDIDATES)
    if logo_path:
        st.image(logo_path, caption=brand, use_container_width=True)

    st.markdown("---")
    st.markdown("### Data Source")
    st.session_state.data_path = st.text_input(
        "Inventory file path (.csv or .xlsx)",
        value=st.session_state.data_path,
        help="Choose a shared path if multiple users edit."
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Load / Reload", use_container_width=True):
            st.session_state.df = coerce_schema(read_any(st.session_state.data_path))
            st.session_state.dirty = False
            st.toast("Inventory loaded.", icon="✅")
    with c2:
        if st.button("Snapshot CSV", help="Write a timestamped CSV to /snapshots", use_container_width=True):
            snap_path = os.path.join(
                SNAPSHOT_DIR, f"inventory_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            st.session_state.df.to_csv(snap_path, index=False)
            st.success(f"Snapshot saved: {snap_path}")

    st.markdown("---")
    st.session_state.autosave = st.toggle("Autosave after edits", value=st.session_state.autosave)

    st.markdown("---")
    st.markdown("### Sort")
    st.session_state.sort_cols = st.multiselect(
        "Sort by (top to bottom priority)",
        options=[c for c in USER_COLUMNS if c != "Last Updated"],
        default=st.session_state.sort_cols,
    )
    st.session_state.sort_asc = st.radio("Direction", ["Ascending", "Descending"], index=0, horizontal=True) == "Ascending"

# -------------------------- HEADER ----------------------------

left, mid, right = st.columns([0.08, 0.72, 0.2])
with left:
    if logo_path:
        st.image(logo_path, use_container_width=True)
with mid:
    st.title(APP_TITLE)
    st.caption("Edit inline. Rename, change quantities, add new rows, or delete rows — then Save.")
with right:
    base_df = st.session_state.df
    total_items = len(base_df.index)
    total_qty = int(base_df["Qty On Hand"].sum())
    stock_value = float((base_df["Qty On Hand"] * base_df["Unit Cost (ZAR)"]).sum())
    st.metric("Items", f"{total_items:,}")
    st.metric("Total Qty", f"{total_qty:,}")
    st.metric("Stock Value (R)", f"{stock_value:,.2f}")

# ----------------------- FILTERS & VIEW -----------------------

with st.expander("🔎 Filters", expanded=False):
    fcols = st.columns([1, 1, 1, 2])
    with fcols[0]:
        f_cat = st.multiselect("Category", sorted([c for c in base_df["Category"].unique() if c]))
    with fcols[1]:
        f_loc = st.multiselect("Location", sorted([c for c in base_df["Location"].unique() if c]))
    with fcols[2]:
        f_uom = st.multiselect("UOM", sorted([c for c in base_df["UOM"].unique() if c]))
    with fcols[3]:
        q = st.text_input("Search (Code / Description)")

view_df = base_df.copy()
if f_cat:
    view_df = view_df[view_df["Category"].isin(f_cat)]
if f_loc:
    view_df = view_df[view_df["Location"].isin(f_loc)]
if f_uom:
    view_df = view_df[view_df["UOM"].isin(f_uom)]
if q.strip():
    qq = q.strip().lower()
    view_df = view_df[
        view_df["Item Code"].str.lower().str.contains(qq, na=False)
        | view_df["Description"].str.lower().str.contains(qq, na=False)
    ]

# Apply sort for display
view_df = apply_sort(view_df, st.session_state.sort_cols, st.session_state.sort_asc)

st.markdown("### Inventory")
st.caption(
    "• Double-click to edit cells.  • Use the last empty row to add items.  • Tick **Delete?** to mark rows for deletion."
)

# Ensure editor includes only relevant columns (keep extras but show ours first)
leading_show = [ROW_ID_COL, DELETE_COL] + [c for c in USER_COLUMNS]
extras_show = [c for c in view_df.columns if c not in leading_show]
editor_df = view_df[leading_show + extras_show].copy()

edited_df = st.data_editor(
    editor_df,
    key="inventory_editor_v2",
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        ROW_ID_COL: st.column_config.TextColumn(label="row id", disabled=True),
        DELETE_COL: st.column_config.CheckboxColumn(label="Delete?"),
        "Qty On Hand": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Min Level": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Max Level": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Unit Cost (ZAR)": st.column_config.NumberColumn(format="%.2f", step=0.10, min_value=0.0),
        "Last Updated": st.column_config.TextColumn(disabled=True),
    },
)

# -------------------- APPLY EDITS PRECISELY -------------------

def merge_edits(master_df: pd.DataFrame, display_before: pd.DataFrame, edited_view: pd.DataFrame) -> pd.DataFrame:
    """
    Apply edits from 'edited_view' back to 'master_df' using the stable hidden ROW_ID_COL.
    Works even when a filter/sort was active.
    """
    master = master_df.copy()

    # Ensure schema/dtypes
    edited = coerce_schema(edited_view.drop(columns=[c for c in edited_view.columns if c not in master.columns], errors="ignore"))
    master = coerce_schema(master)

    # 1) DELETE rows ticked in the editor (only those present in the edited view)
    to_delete_ids = edited.loc[edited[DELETE_COL] == True, ROW_ID_COL].astype(str).tolist()
    if to_delete_ids:
        master = master[~master[ROW_ID_COL].astype(str).isin(to_delete_ids)].copy()

    # 2) UPDATE existing rows (match by _row_id)
    existing = edited[(edited[ROW_ID_COL].astype(str).str.len() > 0) & (~edited[ROW_ID_COL].isna())].copy()
    existing = existing.drop(columns=[DELETE_COL], errors="ignore")

    if not existing.empty:
        m_idx = master.set_index(ROW_ID_COL)
        e_idx = existing.set_index(ROW_ID_COL)

        # Align columns
        use_cols = [c for c in e_idx.columns if c in m_idx.columns]
        m_idx.update(e_idx[use_cols])
        master = m_idx.reset_index()

    # 3) APPEND new rows (those without _row_id got added by the user in the editor)
    new_rows = edited[(edited[ROW_ID_COL].isna()) | (edited[ROW_ID_COL].astype(str).str.len() == 0)].copy()
    if not new_rows.empty:
        # Minimum info to keep a row: Item Code or Description
        keep_mask = (new_rows["Item Code"].astype(str).str.len() > 0) | (new_rows["Description"].astype(str).str.len() > 0)
        new_rows = new_rows[keep_mask].copy()
        if not new_rows.empty:
            # Generate row ids
            new_ids = [f"opw_{uuid.uuid4().hex}" for _ in range(len(new_rows))]
            new_rows[ROW_ID_COL] = new_ids
            new_rows[DELETE_COL] = False
            master = pd.concat([master, new_rows.reindex(columns=master.columns, fill_value="")], ignore_index=True)

    # Basic sanitation
    master = coerce_schema(master)

    # 4) Optional: enforce no negative numbers (already handled by editor, but just in case)
    for c in INT_COLS:
        master[c] = master[c].clip(lower=0)
    master["Unit Cost (ZAR)"] = master["Unit Cost (ZAR)"].clip(lower=0.0)

    return master

# Buttons
c_a, c_b, c_c, c_d = st.columns([1, 1, 1, 1])
with c_a:
    save_clicked = st.button("💾 Save", type="primary", use_container_width=True)
with c_b:
    export_clicked = st.button("⬇️ Export Excel", use_container_width=True)
with c_c:
    clear_filters = st.button("🔁 Clear Filters", use_container_width=True)
with c_d:
    rebuild_ids = st.button("🆔 Rebuild Missing IDs", help="Generate Item ID for blank entries", use_container_width=True)

if clear_filters:
    st.experimental_rerun()

# Merge live edits into session master DF
st.session_state.df = merge_edits(st.session_state.df, editor_df, edited_df)

# If autosave, persist immediately after merge
if st.session_state.autosave:
    try:
        write_any(apply_sort(st.session_state.df, st.session_state.sort_cols, st.session_state.sort_asc), st.session_state.data_path)
        st.toast("Autosaved.", icon="💾")
    except Exception as e:
        st.error(f"Autosave failed: {e}")

# Manual save
if save_clicked:
    try:
        # Update Last Updated now (handled inside write_any)
        sorted_df = apply_sort(st.session_state.df, st.session_state.sort_cols, st.session_state.sort_asc)
        write_any(sorted_df, st.session_state.data_path)
        st.session_state.df = coerce_schema(read_any(st.session_state.data_path))  # re-read for absolute certainty
        st.success("Saved successfully.")
    except Exception as e:
        st.error(f"Save failed: {e}")

# Export
if export_clicked:
    try:
        bin_xlsx = export_excel_bytes(apply_sort(st.session_state.df, st.session_state.sort_cols, st.session_state.sort_asc))
        st.download_button(
            "Download Inventory.xlsx",
            data=bin_xlsx,
            file_name=f"Inventory_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Export failed: {e}")

# Rebuild Item ID helper (optional field)
if rebuild_ids:
    df = st.session_state.df.copy()
    if "Item ID" in df.columns:
        needs = df["Item ID"].astype(str).str.len() == 0
        if needs.any():
            stamp = datetime.now().strftime("%y%m%d%H%M%S")
            seq = 1
            for i in df.index[needs]:
                df.at[i, "Item ID"] = f"OPW-{stamp}-{seq:03d}"
                seq += 1
            st.session_state.df = df
            st.toast("Item IDs generated.", icon="🆔")
    else:
        st.info("Column 'Item ID' not present; nothing to rebuild.")

st.markdown("---")
st.caption("© OpperWorks — Stable inline editing with precise updates, deletes, reordering, backups, and snapshots.")
