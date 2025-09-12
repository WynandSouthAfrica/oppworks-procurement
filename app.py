# app.py
# OpperWorks Stock Take — Streamlit Inventory Editor
# v1.3 — fixes edit persistence + adds autosave, backups, snapshots, and branding

import os
import io
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

NUMERIC_COLS = ["Qty On Hand", "Min Level", "Max Level", "Unit Cost (ZAR)"]
INT_COLS = ["Qty On Hand", "Min Level", "Max Level"]
FLOAT_COLS = ["Unit Cost (ZAR)"]

DEFAULT_COLUMNS = [
    "Item ID",
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


def now_ts() -> str:
    # Use user's timezone implicitly (Streamlit server locale). Avoid tz libs for portability.
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=DEFAULT_COLUMNS)
    return df


def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure required columns exist
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Coerce numeric types
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype("float64")

    # Clean strings and strip whitespace
    for c in ["Item ID", "Item Code", "Description", "Category", "UOM", "Location"]:
        df[c] = df[c].astype("string").fillna("").str.strip()

    # Last Updated
    if "Last Updated" in df.columns:
        df["Last Updated"] = df["Last Updated"].astype("string").fillna("")

    # Ensure column order
    df = df[DEFAULT_COLUMNS]
    return df


def load_or_create(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        if path.lower().endswith(".xlsx"):
            df = pd.read_excel(path)
        else:
            df = pd.read_csv(path)
    else:
        df = init_df()
        save_df(df, path, make_backup=False)
    return coerce_dtypes(df)


def backup_path(base_dir: str, stem: str, ext: str = ".csv") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(base_dir, f"{stem}_{ts}{ext}")


def save_df(df: pd.DataFrame, path: str, make_backup: bool = True):
    # Basic sanitization
    df = coerce_dtypes(df).copy()
    # Update Last Updated on all rows to ensure clear provenance after edit
    df["Last Updated"] = now_ts()

    # Backup current file if exists
    if make_backup and os.path.exists(path):
        stem = os.path.splitext(os.path.basename(path))[0]
        bkp = backup_path(BACKUP_DIR, stem, ".csv")
        try:
            current = pd.read_excel(path) if path.lower().endswith(".xlsx") else pd.read_csv(path)
            current.to_csv(bkp, index=False)
        except Exception:
            # If the existing file is corrupt or can't be read, still proceed to write new data
            pass

    # Persist
    if path.lower().endswith(".xlsx"):
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Inventory")
    else:
        df.to_csv(path, index=False)


def export_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Inventory")
        # Optional: simple number formatting
        wb = writer.book
        money_fmt = wb.add_format({"num_format": "#,##0.00"})
        qty_fmt = wb.add_format({"num_format": "0"})
        ws = writer.sheets["Inventory"]
        # Apply formats by column index
        col_idx = {col: i for i, col in enumerate(df.columns)}
        if "Unit Cost (ZAR)" in col_idx:
            ws.set_column(col_idx["Unit Cost (ZAR)"], col_idx["Unit Cost (ZAR)"], 14, money_fmt)
        for c in ["Qty On Hand", "Min Level", "Max Level"]:
            if c in col_idx:
                ws.set_column(col_idx[c], col_idx[c], 12, qty_fmt)
    out.seek(0)
    return out.read()


def normalize_editor_output(df: pd.DataFrame) -> pd.DataFrame:
    # Remove empty trailing rows (where all fields are blank)
    # But keep rows that at least have an Item Code or Description
    mask_keep = (
        df["Item Code"].astype(str).str.len() > 0
    ) | (df["Description"].astype(str).str.len() > 0)
    df = df[mask_keep].copy()

    # Auto-generate Item ID if missing
    if "Item ID" not in df.columns:
        df["Item ID"] = ""
    df["Item ID"] = df["Item ID"].fillna("")
    needs_id = df["Item ID"] == ""
    if needs_id.any():
        # Simple incremental ID with timestamp stem
        ts = datetime.now().strftime("%y%m%d%H%M%S")
        start_seq = 1
        existing_ids = set(df.loc[~needs_id, "Item ID"].tolist())
        for idx in df.index[df["Item ID"] == ""]:
            new_id = f"OPW-{ts}-{start_seq:03d}"
            while new_id in existing_ids:
                start_seq += 1
                new_id = f"OPW-{ts}-{start_seq:03d}"
            df.at[idx, "Item ID"] = new_id
            existing_ids.add(new_id)

    # No negative quantities
    for c in INT_COLS:
        df[c] = df[c].clip(lower=0)

    # Costs cannot be negative
    df["Unit Cost (ZAR)"] = df["Unit Cost (ZAR)"].clip(lower=0.0)

    # Enforce dtypes + column order
    return coerce_dtypes(df)


def duplicates_in(df: pd.DataFrame, column: str) -> List[str]:
    if column not in df.columns:
        return []
    d = df[column].astype(str).str.upper()
    dupes = d[d.duplicated(keep=False)]
    return sorted(dupes.unique().tolist())


# ------------------------- UI LAYOUT --------------------------

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📦",
    layout="wide",
)

ensure_dirs()

# Sidebar brand/logo
with st.sidebar:
    st.markdown("### Branding")
    brand = st.selectbox("Select brand logo", ["OpperWorks", "PG Bison", "None"], index=0, key="brand_select")

    logo_path = None
    if brand == "OpperWorks":
        logo_path = find_first_existing(OPPERWORKS_LOGO_CANDIDATES)
    elif brand == "PG Bison":
        logo_path = find_first_existing(PG_BISON_LOGO_CANDIDATES)

    if logo_path:
        st.image(logo_path, caption=brand, use_container_width=True)

    st.markdown("---")
    st.markdown("### Data Source")

    data_path = st.text_input(
        "Inventory file path (.csv or .xlsx)",
        value=st.session_state.get("data_path", DEFAULT_DATA_PATH),
        key="data_path_input",
        help="Use a shared path if multiple people will edit. CSV is fastest.",
    )
    st.session_state["data_path"] = data_path

    col_sb1, col_sb2 = st.columns([1, 1])
    with col_sb1:
        load_clicked = st.button("Load / Reload", use_container_width=True)
    with col_sb2:
        snapshot_clicked = st.button("Snapshot", help="Save a timestamped CSV snapshot", use_container_width=True)

    st.markdown("---")
    autosave = st.toggle("Autosave after each edit", value=st.session_state.get("autosave", False), key="autosave_toggle")
    st.session_state["autosave"] = autosave

    st.markdown("---")
    uploaded = st.file_uploader("Import CSV/XLSX (append or replace via options below)", type=["csv", "xlsx"])
    import_mode = st.radio("Import mode", ["Append", "Replace"], horizontal=True)

# Initialize session state for DF / dirty flag
if "df" not in st.session_state:
    st.session_state.df = load_or_create(st.session_state.get("data_path", DEFAULT_DATA_PATH))
if load_clicked:
    st.session_state.df = load_or_create(st.session_state.get("data_path", DEFAULT_DATA_PATH))
    st.session_state.dirty = False
    st.toast("Inventory loaded.", icon="✅")

if "dirty" not in st.session_state:
    st.session_state.dirty = False

def mark_dirty():
    st.session_state.dirty = True

# Header
left, mid, right = st.columns([0.08, 0.72, 0.2])
with left:
    if logo_path:
        st.image(logo_path, use_container_width=True)
with mid:
    st.title(APP_TITLE)
    st.caption("Editable inventory with reliable save, backups, and quick exports.")
with right:
    # Simple KPIs
    df_for_kpi = st.session_state.df
    total_items = len(df_for_kpi.index)
    total_qty = int(df_for_kpi["Qty On Hand"].sum()) if "Qty On Hand" in df_for_kpi else 0
    stock_value = float((df_for_kpi["Qty On Hand"] * df_for_kpi["Unit Cost (ZAR)"]).sum()) if set(["Qty On Hand", "Unit Cost (ZAR)"]).issubset(df_for_kpi.columns) else 0.0
    st.metric("Items", f"{total_items:,}")
    st.metric("Total Qty", f"{total_qty:,}")
    st.metric("Stock Value (R)", f"{stock_value:,.2f}")

# Import logic
if uploaded is not None:
    try:
        if uploaded.name.lower().endswith(".xlsx"):
            new_df = pd.read_excel(uploaded)
        else:
            new_df = pd.read_csv(uploaded)

        new_df = coerce_dtypes(new_df)

        if import_mode == "Replace":
            st.session_state.df = new_df
        else:
            # Append (align columns)
            base = st.session_state.df.copy()
            new_df = new_df.reindex(columns=base.columns, fill_value="")
            st.session_state.df = pd.concat([base, new_df], ignore_index=True)
        st.session_state.dirty = True
        st.success(f"Imported {len(new_df)} rows ({import_mode.lower()}). Review below and click Save.")
    except Exception as e:
        st.error(f"Import failed: {e}")

# Filters
with st.expander("🔎 Filters & Search", expanded=False):
    fcols = st.columns([1, 1, 1, 2])
    with fcols[0]:
        cat_filter = st.multiselect("Category", sorted([c for c in st.session_state.df["Category"].unique() if c]), default=[])
    with fcols[1]:
        loc_filter = st.multiselect("Location", sorted([c for c in st.session_state.df["Location"].unique() if c]), default=[])
    with fcols[2]:
        uom_filter = st.multiselect("UOM", sorted([c for c in st.session_state.df["UOM"].unique() if c]), default=[])
    with fcols[3]:
        search = st.text_input("Search (Code / Description)", "")

    filt_df = st.session_state.df.copy()
    if len(cat_filter):
        filt_df = filt_df[filt_df["Category"].isin(cat_filter)]
    if len(loc_filter):
        filt_df = filt_df[filt_df["Location"].isin(loc_filter)]
    if len(uom_filter):
        filt_df = filt_df[filt_df["UOM"].isin(uom_filter)]
    if search.strip():
        q = search.strip().lower()
        filt_df = filt_df[
            filt_df["Item Code"].str.lower().str.contains(q, na=False) |
            filt_df["Description"].str.lower().str.contains(q, na=False)
        ]

# Editable table
st.markdown("### Inventory")
help_text = (
    "• Double-click to edit cells.  • Use the last empty row to add items.  "
    "• Negative quantities/costs are blocked.  • 'Item ID' auto-generates if left empty on save."
)
st.caption(help_text)

edited_df = st.data_editor(
    filt_df,
    key="inventory_editor",
    use_container_width=True,
    num_rows="dynamic",
    on_change=mark_dirty,
    column_config={
        "Qty On Hand": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Min Level": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Max Level": st.column_config.NumberColumn(format="%d", step=1, min_value=0),
        "Unit Cost (ZAR)": st.column_config.NumberColumn(format="%.2f", step=0.10, min_value=0.0),
        "Last Updated": st.column_config.TextColumn(disabled=True),
    },
)

# IMPORTANT: Merge edits back into the full dataframe (respecting filters)
# Strategy: Replace rows by Item ID if present; otherwise by index alignment fallback.
def merge_back(full_df: pd.DataFrame, view_df: pd.DataFrame, edited_view_df: pd.DataFrame) -> pd.DataFrame:
    # Determine the index mapping from view_df back to full_df
    # Use Item ID as strong key where available; else align by the original index labels present in the view.
    full = full_df.copy()
    edited = edited_view_df.copy()

    if "Item ID" in full.columns and "Item ID" in edited.columns:
        # Update matching Item IDs
        # First: rows having Item ID (existing), merge values
        existing_ids = edited["Item ID"].astype(str)
        mask_existing = existing_ids.str.len() > 0
        existing_subset = edited[mask_existing]
        if not existing_subset.empty:
            # Set index to Item ID and update
            full_idxed = full.set_index("Item ID")
            incoming_idxed = existing_subset.set_index("Item ID")
            # Align columns
            incoming_idxed = incoming_idxed.reindex(columns=full_idxed.columns)
            full_idxed.update(incoming_idxed)
            full = full_idxed.reset_index()

        # New rows with no Item ID will be appended later when we normalize/save
        new_rows = edited[~mask_existing].copy()
        if not new_rows.empty:
            # Align columns and append
            new_rows = new_rows.reindex(columns=full.columns, fill_value="")
            full = pd.concat([full, new_rows], ignore_index=True)

    else:
        # Fallback: replacement by position for the visible subset,
        # then union with untouched full rows that weren't in the filter.
        # Identify rows in full that match (Item Code + Description) pairs from original view
        orig_keys = set(
            tuple(str(a) for a in r)
            for r in view_df[["Item Code", "Description"]].fillna("").to_records(index=False)
        )
        # Remove any rows in full that match orig_keys
        keep_mask = ~(
            full[["Item Code", "Description"]].fillna("").apply(
                lambda s: (str(s["Item Code"]), str(s["Description"])) in orig_keys, axis=1
            )
        )
        rest = full[keep_mask].copy()
        # Then append edited rows
        edited = edited.reindex(columns=full.columns, fill_value="")
        full = pd.concat([rest, edited], ignore_index=True)

    return full

# Merge the edited view back to master df stored in session
st.session_state.df = merge_back(st.session_state.df, filt_df, edited_df)
st.session_state.df = normalize_editor_output(st.session_state.df)

# Duplicates check (advisory)
dupe_codes = duplicates_in(st.session_state.df, "Item Code")
if dupe_codes:
    st.warning(f"Duplicate Item Codes detected: {', '.join(dupe_codes)}")

# Action buttons
col_a, col_b, col_c, col_d, col_e = st.columns([1, 1, 1, 1, 1])
with col_a:
    save_clicked = st.button("💾 Save Changes", type="primary", use_container_width=True)
with col_b:
    export_clicked = st.button("⬇️ Export Excel", use_container_width=True)
with col_c:
    new_snapshot_clicked = st.button("📸 Quick Snapshot (CSV)", use_container_width=True)
with col_d:
    clear_filters = st.button("🔁 Clear Filters", use_container_width=True)
with col_e:
    gen_ids = st.button("🔧 Rebuild Missing Item IDs", use_container_width=True)

if clear_filters:
    st.rerun()

if gen_ids:
    st.session_state.df = normalize_editor_output(st.session_state.df)
    st.session_state.dirty = True
    st.toast("Missing Item IDs generated.", icon="🆔")

# Autosave behavior
if st.session_state.get("autosave", False) and st.session_state.get("dirty", False):
    try:
        save_df(st.session_state.df, st.session_state.get("data_path", DEFAULT_DATA_PATH))
        st.session_state.dirty = False
        st.toast("Autosaved.", icon="💾")
    except Exception as e:
        st.error(f"Autosave failed: {e}")

# Explicit Save
if save_clicked:
    try:
        save_df(st.session_state.df, st.session_state.get("data_path", DEFAULT_DATA_PATH))
        st.session_state.dirty = False
        st.success("Inventory saved successfully.")
    except Exception as e:
        st.error(f"Save failed: {e}")

# Export
if export_clicked:
    try:
        bin_xlsx = export_excel_bytes(st.session_state.df)
        st.download_button(
            label="Download Inventory.xlsx",
            data=bin_xlsx,
            file_name=f"Inventory_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Export failed: {e}")

# Snapshots
if snapshot_clicked or new_snapshot_clicked:
    try:
        snap_path = backup_path(SNAPSHOT_DIR, "inventory_snapshot", ".csv")
        st.session_state.df.to_csv(snap_path, index=False)
        st.success(f"Snapshot saved: {snap_path}")
    except Exception as e:
        st.error(f"Snapshot failed: {e}")

# Footer info
st.markdown("---")
st.caption(
    "© OpperWorks — Reliable inventory editing with persistent saves. "
    "Data stored in your chosen CSV/XLSX file with timestamped backups."
)
