# OppWorks Procurement App — v0.2-hosted
# Author: ChatGPT (Developer Hat)
# Notes:
# - Persistent storage via OPP_DATA_ROOT env var (e.g., /persist on Render).
# - Creates per-project folders (Quote/Order/Delivery/Invoice).
# - SQLite DB in ROOT/data/procurement.db
# - PDF generation uses fpdf2 and optional brand logo.

import os
import io
import json
import sqlite3
from datetime import datetime, date
from typing import Optional, Tuple

import pandas as pd
from fpdf import FPDF
from PIL import Image
import streamlit as st

APP_VERSION = "v0.2-hosted"

# Storage roots (HOSTED-FRIENDLY)
ROOT = os.environ.get("OPP_DATA_ROOT", os.path.abspath("."))
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "procurement.db")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
ASSETS_DIR = os.path.join(ROOT, "assets")
DEFAULT_STORAGE_ROOT = os.path.join(ROOT, "OppWorks_Procurement")

# Ensure paths exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)
os.makedirs(DEFAULT_STORAGE_ROOT, exist_ok=True)

# -----------------------------
# Utilities
# -----------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def month_text_date(dt: date) -> str:
    # Day Month Year with month spelled out, e.g., 24 August 2025
    return dt.strftime("%d %B %Y")

def load_config() -> dict:
    cfg = {
        "storage_root": DEFAULT_STORAGE_ROOT,
        "brand_logo_path": "",  # e.g., assets/brand_logo.png
        "currency": "ZAR",
        "vat_percent": 15.0,
    }
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                stored = json.load(f)
            cfg.update(stored)
        except Exception:
            pass
    return cfg

def save_config(cfg: dict):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

def ensure_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company TEXT,
            email TEXT,
            phone TEXT,
            created_at TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            location TEXT,
            capex_code TEXT,
            cost_category TEXT, -- Capex / Goods / Services
            root_folder TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approvers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            role TEXT,
            limit_amount REAL NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            item_description TEXT,
            category TEXT, -- Goods / Services
            amount_excl_vat REAL DEFAULT 0,
            vat_percent REAL DEFAULT 15,
            payment_terms TEXT,
            rfq_sent_date TEXT,           -- Day Month Year format
            quote_received_date TEXT,
            requisition_requested_date TEXT,
            order_sent_date TEXT,
            delivered_date TEXT,
            invoice_signed_date TEXT,
            receipting_sent_date TEXT,
            status TEXT,                  -- Derived highest stage
            created_at TEXT NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_id INTEGER NOT NULL,
            doc_type TEXT NOT NULL, -- Quote / Order / Delivery / Invoice
            filename TEXT NOT NULL,
            saved_path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY(purchase_id) REFERENCES purchases(id)
        );
        """
    )

    conn.commit()
    conn.close()

def ensure_project_folders(root_folder: str):
    for sub in ["Quote", "Order", "Delivery", "Invoice"]:
        os.makedirs(os.path.join(root_folder, sub), exist_ok=True)

STATUS_ORDER = [
    ("rfq_sent_date", "RFQ Sent"),
    ("quote_received_date", "Quote Received"),
    ("requisition_requested_date", "Requisition Requested"),
    ("order_sent_date", "Order Sent"),
    ("delivered_date", "Delivered"),
    ("invoice_signed_date", "Invoice Signed"),
    ("receipting_sent_date", "Sent for Receipting"),
]

def derive_status(row: dict) -> str:
    # Returns the highest achieved status in the workflow
    last_status = "Not Started"
    for col, label in STATUS_ORDER:
        if row.get(col):
            last_status = label
    return last_status

def save_uploaded_file(uploaded_file, dest_folder: str) -> Tuple[str, str]:
    os.makedirs(dest_folder, exist_ok=True)
    filename = uploaded_file.name
    dest_path = os.path.join(dest_folder, filename)
    base, ext = os.path.splitext(filename)
    i = 1
    while os.path.exists(dest_path):
        filename = f"{base}_{i}{ext}"
        dest_path = os.path.join(dest_folder, filename)
        i += 1
    with open(dest_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return filename, dest_path

def save_pasted_to_pdf(text: str, dest_folder: str, brand_logo_path: Optional[str]) -> Tuple[str, str]:
    os.makedirs(dest_folder, exist_ok=True)
    filename = f"Pasted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    dest_path = os.path.join(dest_folder, filename)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Header with logo if provided
    if brand_logo_path and os.path.exists(brand_logo_path):
        try:
            pdf.image(brand_logo_path, x=10, y=8, w=30)
        except Exception:
            pass
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, "PG Bison — Pasted Document", ln=True, align="R")
    pdf.ln(10)

    # Body
    pdf.set_font("Arial", size=11)
    for line in text.splitlines():
        pdf.multi_cell(0, 7, line)

    pdf.output(dest_path)
    return filename, dest_path

# -----------------------------
# UI Helpers
# -----------------------------

def currency_fmt(value: float, curr: str = "ZAR") -> str:
    try:
        return f"{curr} {value:,.2f}"
    except Exception:
        return f"{curr} {value}"

def read_df(query: str, params: Tuple = ()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def exec_sql(query: str, params: Tuple = ()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    last_id = cur.lastrowid
    conn.close()
    return last_id

# -----------------------------
# Streamlit App
# -----------------------------

st.set_page_config(
    page_title=f"OppWorks Procurement {APP_VERSION}",
    page_icon="📦",
    layout="wide",
)

ensure_tables()
cfg = load_config()

# Sidebar branding and navigation
with st.sidebar:
    st.title("📦 OppWorks Procurement")
    st.caption(APP_VERSION)

    # Show brand logo if available
    if cfg.get("brand_logo_path") and os.path.exists(cfg["brand_logo_path"]):
        st.image(cfg["brand_logo_path"], use_container_width=True)
    else:
        st.info("Upload a brand logo in Settings to show it here.")

    nav = st.radio(
        "Navigation",
        [
            "Dashboard",
            "Suppliers",
            "Projects",
            "Purchases",
            "Documents",
            "Reports",
            "Settings",
        ],
        index=0,
    )

# -----------------------------
# Dashboard
# -----------------------------
if nav == "Dashboard":
    st.header("Dashboard")
    col1, col2, col3, col4 = st.columns(4)

    total_suppliers = read_df("SELECT COUNT(*) AS n FROM suppliers")["n"][0]
    total_projects = read_df("SELECT COUNT(*) AS n FROM projects")["n"][0]
    total_purchases = read_df("SELECT COUNT(*) AS n FROM purchases")["n"][0]

    df_amount = read_df("SELECT COALESCE(SUM(amount_excl_vat),0) AS amt FROM purchases")
    total_spend = df_amount["amt"][0] if not df_amount.empty else 0

    col1.metric("Suppliers", total_suppliers)
    col2.metric("Projects", total_projects)
    col3.metric("Purchases", total_purchases)
    col4.metric("Spend (excl VAT)", currency_fmt(total_spend, cfg.get("currency", "ZAR")))

    with st.expander("Pipeline by Status", expanded=True):
        df_pipe = read_df(
            "SELECT status, COUNT(*) as n FROM purchases GROUP BY status ORDER BY n DESC"
        )
        st.dataframe(df_pipe, use_container_width=True)

    with st.expander("Recent Purchases"):
        df_recent = read_df(
            """
            SELECT p.id, pj.name AS project, s.company AS supplier, p.item_description, p.status,
                   p.amount_excl_vat, p.created_at
            FROM purchases p
            JOIN projects pj ON pj.id = p.project_id
            JOIN suppliers s ON s.id = p.supplier_id
            ORDER BY p.id DESC LIMIT 25
            """
        )
        st.dataframe(df_recent, use_container_width=True)

# -----------------------------
# Suppliers
# -----------------------------
elif nav == "Suppliers":
    st.header("Suppliers")
    with st.form("add_supplier"):
        st.subheader("Add Supplier")
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Contact Name *")
            company = st.text_input("Company")
        with c2:
            email = st.text_input("Email")
            phone = st.text_input("Phone")
        submitted = st.form_submit_button("Save Supplier")
        if submitted:
            if not name:
                st.error("Contact Name is required")
            else:
                exec_sql(
                    "INSERT INTO suppliers(name, company, email, phone, created_at) VALUES(?,?,?,?,?)",
                    (
                        name.strip(),
                        company.strip(),
                        email.strip(),
                        phone.strip(),
                        month_text_date(date.today()),
                    ),
                )
                st.success("Supplier saved")

    st.subheader("All Suppliers")
    df = read_df("SELECT id, name, company, email, phone, created_at FROM suppliers ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

# -----------------------------
# Projects
# -----------------------------
elif nav == "Projects":
    st.header("Projects")
    with st.form("add_project"):
        st.subheader("Add Project")
        c1, c2, c3 = st.columns(3)
        with c1:
            prj_name = st.text_input("Project Name *")
            location = st.selectbox("Location", ["Boksburg", "Piet Retief", "Ugie", "Other"], index=0)
        with c2:
            capex_code = st.text_input("CAPEX / Cost Code (optional)")
            cost_category = st.selectbox("Cost Category", ["Capex", "Goods", "Services"], index=1)
        with c3:
            root_folder = st.text_input(
                "Project Root Folder (leave blank for default)",
                value="",
                placeholder=cfg.get("storage_root", DEFAULT_STORAGE_ROOT),
            )
        submitted = st.form_submit_button("Create Project & Folders")
        if submitted:
            if not prj_name:
                st.error("Project Name is required")
            else:
                base_root = root_folder.strip() or cfg.get("storage_root", DEFAULT_STORAGE_ROOT)
                prj_root = os.path.join(base_root, prj_name)
                os.makedirs(prj_root, exist_ok=True)
                ensure_project_folders(prj_root)
                exec_sql(
                    """
                    INSERT INTO projects(name, location, capex_code, cost_category, root_folder, created_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        prj_name.strip(),
                        location,
                        capex_code.strip(),
                        cost_category,
                        prj_root,
                        month_text_date(date.today()),
                    ),
                )
                st.success(f"Project created at: {prj_root}")

    st.subheader("All Projects")
    df = read_df(
        "SELECT id, name, location, capex_code, cost_category, root_folder, created_at FROM projects ORDER BY id DESC"
    )
    st.dataframe(df, use_container_width=True)

# -----------------------------
# Purchases
# -----------------------------
elif nav == "Purchases":
    st.header("Purchases / RFQs")

    df_projects = read_df("SELECT id, name FROM projects ORDER BY name")
    df_suppliers = read_df("SELECT id, company || COALESCE(' — '||name,'') AS label FROM suppliers ORDER BY company, name")

    if df_projects.empty or df_suppliers.empty:
        st.warning("Add at least one Project and one Supplier first.")
    else:
        with st.form("add_purchase"):
            st.subheader("New Purchase / RFQ")
            c1, c2 = st.columns([2, 1])
            with c1:
                project_id = st.selectbox("Project *", options=df_projects["id"].tolist(), format_func=lambda i: df_projects.set_index("id").loc[i, "name"])
                supplier_id = st.selectbox("Supplier *", options=df_suppliers["id"].tolist(), format_func=lambda i: df_suppliers.set_index("id").loc[i, "label"])
                item_description = st.text_area("Item / Service Description")
            with c2:
                category = st.selectbox("Category", ["Goods", "Services"], index=0)
                amount_excl = st.number_input("Amount (excl VAT)", min_value=0.0, step=100.0)
                vat_percent = st.number_input("VAT %", min_value=0.0, max_value=100.0, value=float(cfg.get("vat_percent", 15.0)), step=0.5)
                payment_terms = st.text_input("Payment Terms (e.g., 30 days)")

            st.markdown("---")
            st.subheader("Workflow Status Dates (Day Month Year)")
            today = date.today()
            c1, c2, c3 = st.columns(3)
            with c1:
                rfq_sent = st.date_input("RFQ Sent", value=today)
                quote_received = st.date_input("Quote Received")
                requisition_requested = st.date_input("Requisition Requested")
            with c2:
                order_sent = st.date_input("Order Sent")
                delivered = st.date_input("Delivered")
                invoice_signed = st.date_input("Invoice Signed")
            with c3:
                receipting_sent = st.date_input("Sent for Receipting")
                st.caption("Leave blank if a step hasn't happened yet.")

            submitted = st.form_submit_button("Save Purchase")
            if submitted:
                if project_id is None or supplier_id is None:
                    st.error("Project and Supplier are required")
                else:
                    row = {
                        "rfq_sent_date": month_text_date(rfq_sent) if rfq_sent else None,
                        "quote_received_date": month_text_date(quote_received) if quote_received else None,
                        "requisition_requested_date": month_text_date(requisition_requested) if requisition_requested else None,
                        "order_sent_date": month_text_date(order_sent) if order_sent else None,
                        "delivered_date": month_text_date(delivered) if delivered else None,
                        "invoice_signed_date": month_text_date(invoice_signed) if invoice_signed else None,
                        "receipting_sent_date": month_text_date(receipting_sent) if receipting_sent else None,
                    }
                    status = derive_status(row)
                    exec_sql(
                        """
                        INSERT INTO purchases(
                            project_id, supplier_id, item_description, category, amount_excl_vat, vat_percent, payment_terms,
                            rfq_sent_date, quote_received_date, requisition_requested_date, order_sent_date, delivered_date, invoice_signed_date, receipting_sent_date,
                            status, created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(project_id), int(supplier_id), item_description.strip(), category, float(amount_excl), float(vat_percent), payment_terms.strip(),
                            row["rfq_sent_date"], row["quote_received_date"], row["requisition_requested_date"], row["order_sent_date"], row["delivered_date"], row["invoice_signed_date"], row["receipting_sent_date"],
                            status, month_text_date(date.today()),
                        ),
                    )
                    st.success(f"Purchase saved with status: {status}")

        st.subheader("All Purchases")
        dfp = read_df(
            """
            SELECT p.id, pj.name AS project, s.company AS supplier, p.item_description, p.category, p.amount_excl_vat,
                   p.status, p.created_at
            FROM purchases p
            JOIN projects pj ON pj.id = p.project_id
            JOIN suppliers s ON s.id = p.supplier_id
            ORDER BY p.id DESC
            """
        )
        st.dataframe(dfp, use_container_width=True)

# -----------------------------
# Documents
# -----------------------------
elif nav == "Documents":
    st.header("Documents")
    dfp = read_df(
        """
        SELECT p.id, pj.name AS project, pj.root_folder AS prj_root, s.company AS supplier
        FROM purchases p
        JOIN projects pj ON pj.id = p.project_id
        JOIN suppliers s ON s.id = p.supplier_id
        ORDER BY p.id DESC
        """
    )

    if dfp.empty:
        st.warning("No purchases found. Create a purchase first.")
    else:
        purchase_map = {int(row["id"]): f"#{row['id']} — {row['project']} — {row['supplier']}" for _, row in dfp.iterrows()}
        purchase_id = st.selectbox("Select Purchase", options=list(purchase_map.keys()), format_func=lambda i: purchase_map[i])
        doc_type = st.selectbox("Document Type", ["Quote", "Order", "Delivery", "Invoice"]) 

        # File upload OR paste text -> PDF
        st.markdown("**Upload a file** *or* **Paste text to generate PDF**")
        uploaded = st.file_uploader("Upload PDF or any file", type=None)
        pasted_text = st.text_area("Paste text (optional) — will be saved as a PDF with logo")
        do_save = st.button("Save Document")

        if do_save:
            row = dfp[dfp["id"] == purchase_id].iloc[0]
            prj_root = row["prj_root"]
            dest_folder = os.path.join(prj_root, doc_type)

            saved_any = False
            if uploaded is not None:
                fname, fpath = save_uploaded_file(uploaded, dest_folder)
                exec_sql(
                    "INSERT INTO documents(purchase_id, doc_type, filename, saved_path, uploaded_at) VALUES(?,?,?,?,?)",
                    (int(purchase_id), doc_type, fname, fpath, month_text_date(date.today())),
                )
                st.success(f"File saved: {fpath}")
                saved_any = True

            if pasted_text.strip():
                fname, fpath = save_pasted_to_pdf(pasted_text, dest_folder, cfg.get("brand_logo_path"))
                exec_sql(
                    "INSERT INTO documents(purchase_id, doc_type, filename, saved_path, uploaded_at) VALUES(?,?,?,?,?)",
                    (int(purchase_id), doc_type, fname, fpath, month_text_date(date.today())),
                )
                st.success(f"Pasted text saved to PDF: {fpath}")
                saved_any = True

            if not saved_any:
                st.warning("Nothing to save — upload a file or paste text.")

        st.subheader("Recent Documents")
        dfd = read_df(
            """
            SELECT d.id, d.purchase_id, d.doc_type, d.filename, d.saved_path, d.uploaded_at
            FROM documents d
            ORDER BY d.id DESC LIMIT 50
            """
        )
        st.dataframe(dfd, use_container_width=True)

# -----------------------------
# Reports
# -----------------------------
elif nav == "Reports":
    st.header("Reports")

    df_prj = read_df("SELECT id, name FROM projects ORDER BY name")
    if df_prj.empty:
        st.warning("Add a project to view reports.")
    else:
        project_id = st.selectbox("Project", options=df_prj["id"].tolist(), format_func=lambda i: df_prj.set_index("id").loc[i, "name"])

        df_rep = read_df(
            """
            SELECT p.id, s.company AS supplier, p.item_description, p.category, p.amount_excl_vat, p.vat_percent, p.status,
                   p.rfq_sent_date, p.quote_received_date, p.requisition_requested_date, p.order_sent_date, p.delivered_date, p.invoice_signed_date, p.receipting_sent_date
            FROM purchases p
            JOIN suppliers s ON s.id = p.supplier_id
            WHERE p.project_id = ?
            ORDER BY p.id DESC
            """,
            (int(project_id),),
        )

        st.subheader("Project Purchases")
        st.dataframe(df_rep, use_container_width=True)

        total_excl = float(df_rep["amount_excl_vat"].sum()) if not df_rep.empty else 0.0
        total_vat = float((df_rep["amount_excl_vat"] * (df_rep["vat_percent"] / 100.0)).sum()) if not df_rep.empty else 0.0
        total_incl = total_excl + total_vat

        c1, c2, c3 = st.columns(3)
        c1.metric("Total (excl VAT)", currency_fmt(total_excl, cfg.get("currency", "ZAR")))
        c2.metric("VAT", currency_fmt(total_vat, cfg.get("currency", "ZAR")))
        c3.metric("Total (incl VAT)", currency_fmt(total_incl, cfg.get("currency", "ZAR")))

        # CSV export
        csv_bytes = df_rep.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv_bytes, file_name="project_purchases.csv", mime="text/csv")

        # PDF summary
        if st.button("Generate PDF Summary"):
            prj_name = df_prj.set_index("id").loc[int(project_id), "name"]
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()

            if cfg.get("brand_logo_path") and os.path.exists(cfg["brand_logo_path"]):
                try:
                    pdf.image(cfg["brand_logo_path"], x=10, y=8, w=30)
                except Exception:
                    pass

            pdf.set_font("Arial", style="B", size=14)
            pdf.cell(0, 10, f"Project Summary — {prj_name}", ln=True, align="R")
            pdf.ln(5)

            pdf.set_font("Arial", size=11)
            pdf.cell(0, 8, f"Generated: {month_text_date(date.today())}", ln=True)
            pdf.cell(0, 8, f"Totals (excl): {currency_fmt(total_excl, cfg.get('currency','ZAR'))}", ln=True)
            pdf.cell(0, 8, f"VAT: {currency_fmt(total_vat, cfg.get('currency','ZAR'))}", ln=True)
            pdf.cell(0, 8, f"Total (incl): {currency_fmt(total_incl, cfg.get('currency','ZAR'))}", ln=True)
            pdf.ln(4)

            # Table header
            pdf.set_font("Arial", style="B", size=11)
            pdf.cell(15, 8, "ID", border=1)
            pdf.cell(50, 8, "Supplier", border=1)
            pdf.cell(35, 8, "Category", border=1)
            pdf.cell(40, 8, "Amount Excl", border=1)
            pdf.cell(50, 8, "Status", border=1, ln=1)

            pdf.set_font("Arial", size=10)
            for _, r in df_rep.iterrows():
                pdf.cell(15, 8, str(r["id"]), border=1)
                pdf.cell(50, 8, (r["supplier"] or "")[:28], border=1)
                pdf.cell(35, 8, r["category"] or "", border=1)
                pdf.cell(40, 8, currency_fmt(float(r["amount_excl_vat"]), cfg.get("currency","ZAR")), border=1)
                pdf.cell(50, 8, (r["status"] or "")[:28], border=1, ln=1)

            out_path = os.path.join(DATA_DIR, f"project_summary_{prj_name.replace(' ','_')}.pdf")
            pdf.output(out_path)
            with open(out_path, "rb") as f:
                st.download_button("Download PDF Summary", data=f.read(), file_name=os.path.basename(out_path), mime="application/pdf")

# -----------------------------
# Settings
# -----------------------------
elif nav == "Settings":
    st.header("Settings")

    st.subheader("Storage & Branding")
    c1, c2 = st.columns([2,1])
    with c1:
        storage_root = st.text_input("Default Storage Root", value=cfg.get("storage_root", DEFAULT_STORAGE_ROOT))
        currency = st.selectbox("Currency", ["ZAR", "USD", "EUR", "GBP"], index=["ZAR","USD","EUR","GBP"].index(cfg.get("currency","ZAR")))
        vat_percent = st.number_input("Default VAT %", min_value=0.0, max_value=100.0, step=0.5, value=float(cfg.get("vat_percent", 15.0)))
    with c2:
        st.caption("Brand Logo (shown in sidebar & PDFs)")
        logo_file = st.file_uploader("Upload PNG/JPG logo", type=["png","jpg","jpeg"], key="logo_up")
        if st.button("Save Logo") and logo_file is not None:
            ext = os.path.splitext(logo_file.name)[1].lower()
            logo_path = os.path.join(ASSETS_DIR, f"brand_logo{ext}")
            with open(logo_path, "wb") as f:
                f.write(logo_file.getbuffer())
            cfg["brand_logo_path"] = logo_path
            save_config(cfg)
            st.success(f"Logo saved: {logo_path}")
            st.experimental_rerun()

    if st.button("Save Settings"):
        cfg["storage_root"] = storage_root.strip() or DEFAULT_STORAGE_ROOT
        cfg["currency"] = currency
        cfg["vat_percent"] = float(vat_percent)
        save_config(cfg)
        st.success("Settings saved")

    st.divider()
    st.subheader("Approvers & Limits")
    with st.form("add_approver"):
        c1, c2, c3 = st.columns(3)
        with c1:
            ap_name = st.text_input("Name")
        with c2:
            ap_role = st.text_input("Role")
        with c3:
            ap_limit = st.number_input("Limit Amount (excl)", min_value=0.0, step=1000.0)
        submitted = st.form_submit_button("Add Approver")
        if submitted:
            if not ap_name:
                st.error("Name required")
            else:
                exec_sql(
                    "INSERT INTO approvers(name, role, limit_amount, created_at) VALUES(?,?,?,?)",
                    (ap_name.strip(), ap_role.strip(), float(ap_limit), month_text_date(date.today())),
                )
                st.success("Approver saved")

    df_ap = read_df("SELECT id, name, role, limit_amount, created_at FROM approvers ORDER BY id DESC")
    st.dataframe(df_ap, use_container_width=True)
