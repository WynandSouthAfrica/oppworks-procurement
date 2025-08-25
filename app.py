# OppWorks Procurement — v1.0 (Fresh MVP)
# Storage: set OPP_DATA_ROOT (use /persist on Render). Falls back to current folder.
# Core: Suppliers, Projects, Purchases (stages), Documents (versioned), Reports, Settings (snapshots).

import os, io, json, sqlite3, zipfile, shutil
from datetime import date, datetime
from typing import Tuple

import pandas as pd
from fpdf import FPDF
import streamlit as st

APP_VERSION = "v1.0"

# --------------- Paths & config ---------------
ROOT = os.environ.get("OPP_DATA_ROOT", os.path.abspath("."))
DATA_DIR = os.path.join(ROOT, "data")
ASSETS_DIR = os.path.join(ROOT, "assets")
DB_PATH = os.path.join(DATA_DIR, "procurement.db")
CFG_PATH = os.path.join(DATA_DIR, "config.json")
DEFAULT_STORAGE_ROOT = os.path.join(ROOT, "Procurement_Hub")
BACKUPS_DIR = os.path.join(ROOT, "backups")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)
os.makedirs(BACKUPS_DIR, exist_ok=True)
os.makedirs(DEFAULT_STORAGE_ROOT, exist_ok=True)

STAGE_ORDER = [
    ("rfq_sent_date",             "RFQ Sent",               None),
    ("quote_received_date",       "Quote Received",         "Quote"),
    ("requisition_requested_date","Requisition Requested",  "Requisition"),
    ("order_sent_date",           "Order Sent",             "Order"),
    ("delivered_date",            "Delivered",              "Delivery"),
    ("invoice_signed_date",       "Invoice Signed",         "Invoice"),
    ("receipting_sent_date",      "Sent for Receipting",    "Invoice"),
]

DOC_FOLDER = {
    "Quote": "Quote",
    "Requisition": "Requisition",
    "Order": "Order",
    "Delivery": "Delivery",
    "Invoice": "Invoice",
}

# --------------- Utilities ---------------
def month_text(d: date) -> str:
    return d.strftime("%d %B %Y")

def get_conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def read_df(sql: str, params: Tuple = ()):
    con = get_conn()
    df = pd.read_sql_query(sql, con, params=params)
    con.close()
    return df

def exec_sql(sql: str, params: Tuple = ()):
    con = get_conn(); cur = con.cursor()
    cur.execute(sql, params); con.commit()
    lid = cur.lastrowid
    con.close()
    return lid

def ensure_tables():
    con = get_conn(); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS suppliers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        company TEXT,
        email TEXT,
        phone TEXT,
        services TEXT,
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS approvers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        role TEXT,
        limit_amount REAL NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS projects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        location TEXT,
        gl_code TEXT,
        operation_unit TEXT,
        cost_centre TEXT,
        miscellaneous TEXT,
        partner TEXT,
        approver_id INTEGER,
        root_folder TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS purchases(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        supplier_id INTEGER NOT NULL,
        item_description TEXT,
        category TEXT,                      -- Goods / Services
        amount_excl REAL DEFAULT 0,
        vat_percent REAL DEFAULT 15,
        payment_terms TEXT,

        rfq_sent_date TEXT,
        quote_received_date TEXT,
        requisition_requested_date TEXT,
        order_sent_date TEXT,
        delivered_date TEXT,
        invoice_signed_date TEXT,
        receipting_sent_date TEXT,

        status TEXT,                        -- derived
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS documents(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        purchase_id INTEGER NOT NULL,
        doc_type TEXT NOT NULL,             -- Quote, Requisition, Order, Delivery, Invoice
        filename TEXT NOT NULL,
        path TEXT NOT NULL,
        version INTEGER DEFAULT 1,
        is_current INTEGER DEFAULT 1,
        uploaded_at TEXT NOT NULL
    )""")
    con.commit(); con.close()

def load_cfg():
    base = {
        "storage_root": DEFAULT_STORAGE_ROOT,
        "brand_logo": "",
        "currency": "ZAR",
        "vat_percent": 15.0
    }
    if os.path.exists(CFG_PATH):
        try:
            base.update(json.load(open(CFG_PATH, "r", encoding="utf-8")))
        except:
            pass
    return base

def save_cfg(cfg: dict):
    json.dump(cfg, open(CFG_PATH, "w", encoding="utf-8"), indent=2)

def ensure_project_dirs(root_folder: str):
    for sub in DOC_FOLDER.values():
        os.makedirs(os.path.join(root_folder, sub), exist_ok=True)

def latest_status(row: dict) -> str:
    last = "Not Started"
    for col, label, _ in STAGE_ORDER:
        if row.get(col):
            last = label
    return last

def next_version(purchase_id: int, doc_type: str) -> int:
    con = get_conn(); cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(version),0) FROM documents WHERE purchase_id=? AND doc_type=?",
                (purchase_id, doc_type))
    v = (cur.fetchone()[0] or 0) + 1
    con.close()
    return v

def versioned_name(name: str, v: int) -> str:
    b, e = os.path.splitext(name)
    return f"{b}_v{v}{e}"

def save_upload(uploaded, folder: str, v: int) -> Tuple[str, str]:
    os.makedirs(folder, exist_ok=True)
    fname = versioned_name(uploaded.name, v)
    dest = os.path.join(folder, fname)
    with open(dest, "wb") as f:
        f.write(uploaded.getbuffer())
    return fname, dest

def save_text_as_pdf(text: str, folder: str, v: int, logo: str = "") -> Tuple[str, str]:
    os.makedirs(folder, exist_ok=True)
    fname = f"Pasted_v{v}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    dest = os.path.join(folder, fname)
    pdf = FPDF(); pdf.set_auto_page_break(True, 15); pdf.add_page()
    if logo and os.path.exists(logo):
        try:
            pdf.image(logo, x=10, y=8, w=30)
        except:
            pass
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, "Procurement Document", ln=True, align="R")
    pdf.ln(6); pdf.set_font("Arial", size=11)
    for line in text.splitlines():
        pdf.multi_cell(0, 7, line)
    pdf.output(dest)
    return fname, dest

def snapshot_zip() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(BACKUPS_DIR, f"oppworks_snapshot_{ts}.zip")
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for root in [DATA_DIR, cfg.get("storage_root", DEFAULT_STORAGE_ROOT)]:
            for folder, _, files in os.walk(root):
                for f in files:
                    full = os.path.join(folder, f)
                    rel = os.path.relpath(full, ROOT)
                    z.write(full, rel)
    return out

# --------------- App shell ---------------
st.set_page_config(page_title=f"OppWorks Procurement {APP_VERSION}", page_icon="📦", layout="wide")
ensure_tables()
cfg = load_cfg()

with st.sidebar:
    st.title("📦 OppWorks Procurement")
    st.caption(APP_VERSION)
    if cfg.get("brand_logo") and os.path.exists(cfg["brand_logo"]):
        st.image(cfg["brand_logo"], use_container_width=True)
    nav = st.radio(
        "Navigate",
        ["Dashboard", "Suppliers", "Projects", "Purchases", "Documents", "Reports", "Settings"],
        index=0
    )

# --------------- Dashboard ---------------
if nav == "Dashboard":
    st.header("Dashboard")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Suppliers", int(read_df("SELECT COUNT(*) n FROM suppliers")["n"][0]))
    c2.metric("Projects", int(read_df("SELECT COUNT(*) n FROM projects")["n"][0]))
    c3.metric("Purchases", int(read_df("SELECT COUNT(*) n FROM purchases")["n"][0]))
    total = float(read_df("SELECT COALESCE(SUM(amount_excl),0) t FROM purchases")["t"][0])
    c4.metric("Spend (excl VAT)", f"{cfg.get('currency','ZAR')} {total:,.2f}")

    st.subheader("Pipeline by Status")
    df = read_df("SELECT COALESCE(status,'Not Started') status, COUNT(*) n FROM purchases GROUP BY status ORDER BY n DESC")
    st.dataframe(df, use_container_width=True)

    st.subheader("Gaps")
    dfm = read_df("""SELECT p.id, pj.name project, s.company supplier,
                     p.quote_received_date, p.requisition_requested_date, p.order_sent_date,
                     p.delivered_date, p.invoice_signed_date, p.receipting_sent_date
                     FROM purchases p
                     JOIN projects pj ON pj.id=p.project_id
                     JOIN suppliers s ON s.id=p.supplier_id
                     ORDER BY p.id DESC""")
    if not dfm.empty:
        for col, _, _ in STAGE_ORDER[1:]:
            dfm[f"missing_{col}"] = dfm[col].isna() | (dfm[col] == "")
        cols = ["id", "project", "supplier"] + [c for c in dfm.columns if c.startswith("missing_")]
        st.dataframe(dfm[cols], use_container_width=True)
    else:
        st.caption("No purchases yet.")

# --------------- Suppliers ---------------
elif nav == "Suppliers":
    st.header("Suppliers")
    with st.form("sup_add"):
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Contact Name *")
            company = st.text_input("Company")
            email = st.text_input("Email")
        with c2:
            phone = st.text_input("Phone")
            services = st.text_area("Goods/Services Description")
        if st.form_submit_button("Save Supplier") and name:
            exec_sql("INSERT INTO suppliers(name,company,email,phone,services,created_at) VALUES(?,?,?,?,?,?)",
                     (name.strip(), company.strip(), email.strip(), phone.strip(), services.strip(), month_text(date.today())))
            st.success("Supplier saved.")

    suppliers = read_df("SELECT id, company, name, email, phone, services, created_at FROM suppliers ORDER BY id DESC")
    st.dataframe(suppliers, use_container_width=True)

    st.subheader("Supplier PDF")
    if not suppliers.empty:
        sid = st.selectbox("Select", suppliers["id"].tolist(),
                           format_func=lambda i: f"{suppliers.set_index('id').loc[i,'company']} — {suppliers.set_index('id').loc[i,'name']}")
        if st.button("Generate Supplier PDF"):
            row = suppliers.set_index("id").loc[int(sid)]
            # build PDF
            pdf = FPDF(); pdf.add_page(); pdf.set_auto_page_break(True, 15)
            if cfg.get("brand_logo") and os.path.exists(cfg["brand_logo"]):
                try: pdf.image(cfg["brand_logo"], x=10, y=8, w=30)
                except: pass
            pdf.set_font("Arial", "B", 14); pdf.cell(0, 10, "Supplier Summary", ln=True, align="R"); pdf.ln(6)
            pdf.set_font("Arial", size=11)
            for k, v in [
                ("Company", row["company"]), ("Contact", row["name"]), ("Email", row["email"]),
                ("Phone", row["phone"]), ("Goods/Services", row["services"])
            ]:
                pdf.set_font("Arial", "B", 11); pdf.cell(45, 8, f"{k}:", 0)
                pdf.set_font("Arial", size=11); pdf.multi_cell(0, 8, str(v or ""))
            out_dir = os.path.join(cfg.get("storage_root", DEFAULT_STORAGE_ROOT), "_Suppliers")
            os.makedirs(out_dir, exist_ok=True)
            out = os.path.join(out_dir, f"Supplier_{(row['company'] or row['name']).replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}.pdf")
            pdf.output(out)
            st.success(f"Saved to {out}")
            with open(out, "rb") as f:
                st.download_button("Download PDF", f.read(), file_name=os.path.basename(out), mime="application/pdf")

# --------------- Projects ---------------
elif nav == "Projects":
    st.header("Projects")

    # approvers (simple for now; can expand later)
    approvers_df = read_df("SELECT id, name, role, limit_amount FROM approvers ORDER BY name")

    with st.form("prj_add"):
        st.subheader("Add Project")
        name = st.text_input("Project name *")
        location = st.selectbox("Location", ["Boksburg", "Piet Retief", "Ugie", "Other"], index=0)
        gl_code = st.text_input("GL-Code")
        operation_unit = st.text_input("Operation Unit")
        cost_centre = st.text_input("Cost Centre")
        miscellaneous = st.text_input("Miscellaneous")
        partner = st.text_input("Partner")
        appr_id = st.selectbox(
            "Approver (optional)",
            [None] + approvers_df["id"].tolist(),
            format_func=lambda i: "—" if i is None else f"{approvers_df.set_index('id').loc[i,'name']} ({approvers_df.set_index('id').loc[i,'role']})"
        )
        root_override = st.text_input("Project Root Folder (leave blank for default)")
        if st.form_submit_button("Create Project & Folders") and name:
            base = root_override.strip() or cfg.get("storage_root", DEFAULT_STORAGE_ROOT)
            prj_root = os.path.join(base, name.strip())
            os.makedirs(prj_root, exist_ok=True)
            ensure_project_dirs(prj_root)
            exec_sql("""INSERT INTO projects(name,location,gl_code,operation_unit,cost_centre,miscellaneous,partner,approver_id,root_folder,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                     (name.strip(), location, gl_code.strip(), operation_unit.strip(), cost_centre.strip(),
                      miscellaneous.strip(), partner.strip(), int(appr_id) if appr_id else None,
                      prj_root, month_text(date.today())))
            st.success(f"Project created at: {prj_root}")

    st.subheader("All Projects")
    projects = read_df("""SELECT p.id, p.name, p.location, p.gl_code, p.operation_unit, p.cost_centre,
                          p.partner, p.root_folder, p.created_at,
                          a.name approver
                          FROM projects p LEFT JOIN approvers a ON a.id=p.approver_id
                          ORDER BY p.id DESC""")
    st.dataframe(projects, use_container_width=True)

# --------------- Purchases ---------------
elif nav == "Purchases":
    st.header("Purchases")

    prj = read_df("SELECT id, name, root_folder FROM projects ORDER BY name")
    sup = read_df("SELECT id, company, name FROM suppliers ORDER BY company, name")

    tabs = st.tabs(["New Purchase", "Manage Purchases"])

    with tabs[0]:
        if prj.empty or sup.empty:
            st.warning("Add at least one Project and one Supplier first.")
        else:
            with st.form("purch_new"):
                c1, c2 = st.columns([2, 1])
                with c1:
                    project_id = st.selectbox("Project *", prj["id"].tolist(), format_func=lambda i: prj.set_index("id").loc[i, "name"])
                    supplier_id = st.selectbox("Supplier *", sup["id"].tolist(), format_func=lambda i: f"{sup.set_index('id').loc[i,'company']} — {sup.set_index('id').loc[i,'name']}")
                    item_desc = st.text_area("Item / Service Description")
                with c2:
                    category = st.selectbox("Category", ["Goods", "Services"], index=0)
                    amount_excl = st.number_input("Amount (excl VAT)", min_value=0.0, step=100.0)
                    vat_percent = st.number_input("VAT %", min_value=0.0, max_value=100.0, value=float(cfg.get("vat_percent", 15.0)))
                    payment_terms = st.text_input("Payment Terms")
                if st.form_submit_button("Create Purchase"):
                    # initial timeline (only RFQ today by default)
                    row = {"rfq_sent_date": month_text(date.today())}
                    status = latest_status(row)
                    exec_sql("""INSERT INTO purchases(project_id,supplier_id,item_description,category,amount_excl,vat_percent,payment_terms,
                                rfq_sent_date,status,created_at)
                                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                             (int(project_id), int(supplier_id), item_desc.strip(), category, float(amount_excl),
                              float(vat_percent), payment_terms.strip(),
                              row["rfq_sent_date"], status, month_text(date.today())))
                    st.success("Purchase created.")

    with tabs[1]:
        # Picker
        dfp = read_df("""SELECT p.id, pj.name project, s.company supplier, p.status, pj.root_folder prj_root
                         FROM purchases p JOIN projects pj ON pj.id=p.project_id
                         JOIN suppliers s ON s.id=p.supplier_id
                         ORDER BY p.id DESC""")
        if dfp.empty:
            st.info("No purchases yet.")
        else:
            pid = st.selectbox("Select purchase", dfp["id"].tolist(),
                               format_func=lambda i: f"#{i} — {dfp.set_index('id').loc[i,'project']} / {dfp.set_index('id').loc[i,'supplier']} [{dfp.set_index('id').loc[i,'status']}]")
            prj_root = dfp.set_index("id").loc[int(pid), "prj_root"]
            details = read_df("SELECT * FROM purchases WHERE id=?", (int(pid),))
            row = details.iloc[0].to_dict()

            st.markdown("### Timeline & Documents")
            for col, label, default_doc in STAGE_ORDER:
                b1, b2 = st.columns([1, 3])
                with b1:
                    st.write(f"**{label}**")
                    if st.button("Mark today", key=f"mark_{col}"):
                        exec_sql(f"UPDATE purchases SET {col}=?, status=? WHERE id=?",
                                 (month_text(date.today()),
                                  latest_status({**row, col: month_text(date.today())}),
                                  int(pid)))
                        st.experimental_rerun()
                with b2:
                    current_val = row.get(col) or ""
                    st.text_input("Date", value=current_val, key=f"show_{col}", disabled=True)
                    # document upload (optional for stages that have docs)
                    if default_doc:
                        up = st.file_uploader(f"Upload {default_doc}", key=f"file_{col}")
                        tx = st.text_area(f"Paste → PDF ({default_doc})", key=f"text_{col}")
                        if st.button(f"Save {default_doc}", key=f"save_{col}"):
                            v = next_version(int(pid), default_doc)
                            dest_dir = os.path.join(prj_root, DOC_FOLDER[default_doc])
                            if up is not None:
                                fname, fpath = save_upload(up, dest_dir, v)
                            elif tx.strip():
                                fname, fpath = save_text_as_pdf(tx, dest_dir, v, cfg.get("brand_logo"))
                            else:
                                st.warning("No file/text provided.")
                                st.stop()
                            doc_id = exec_sql("""INSERT INTO documents(purchase_id,doc_type,filename,path,version,is_current,uploaded_at)
                                                 VALUES(?,?,?,?,?,1,?)""",
                                              (int(pid), default_doc, fname, fpath, v, month_text(date.today())))
                            exec_sql("UPDATE documents SET is_current=0 WHERE purchase_id=? AND doc_type=? AND id<>?",
                                     (int(pid), default_doc, int(doc_id)))
                            st.success(f"Saved {default_doc} v{v}: {fname}")

            # refresh button
            st.button("Refresh", on_click=lambda: st.experimental_rerun())

# --------------- Documents ---------------
elif nav == "Documents":
    st.header("Documents")
    dfd = read_df("""SELECT d.purchase_id, pj.name project, s.company supplier, d.doc_type,
                     MAX(CASE WHEN is_current=1 THEN version ELSE 0 END) AS current_version,
                     MAX(version) AS latest_version
                     FROM documents d
                     JOIN purchases p ON p.id=d.purchase_id
                     JOIN projects pj ON pj.id=p.project_id
                     JOIN suppliers s ON s.id=p.supplier_id
                     GROUP BY d.purchase_id, d.doc_type
                     ORDER BY d.purchase_id DESC, d.doc_type""")
    st.dataframe(dfd, use_container_width=True)

    st.subheader("History & Download")
    pid = st.number_input("Purchase ID", min_value=1, step=1)
    docs = read_df("SELECT id, doc_type, filename, path, version, is_current, uploaded_at FROM documents WHERE purchase_id=? ORDER BY doc_type, version DESC", (int(pid),))
    if docs.empty:
        st.caption("No documents for that ID yet.")
    else:
        st.dataframe(docs.drop(columns=["path"]), use_container_width=True)
        for _, r in docs.iterrows():
            p = r["path"]
            if os.path.exists(p):
                with open(p, "rb") as f:
                    st.download_button(f"Download {r['doc_type']} v{r['version']} — {r['filename']}",
                                       f.read(), file_name=os.path.basename(p), mime="application/octet-stream",
                                       key=f"dl_{r['id']}")

# --------------- Reports ---------------
elif nav == "Reports":
    st.header("Reports")
    prj = read_df("SELECT id, name FROM projects ORDER BY name")
    if prj.empty:
        st.info("Add a project first.")
    else:
        pid = st.selectbox("Project", prj["id"].tolist(), format_func=lambda i: prj.set_index("id").loc[i,"name"])
        df = read_df("""SELECT p.id, s.company supplier, p.item_description, p.category,
                        p.amount_excl, p.vat_percent, p.status
                        FROM purchases p JOIN suppliers s ON s.id=p.supplier_id
                        WHERE p.project_id=? ORDER BY p.id DESC""", (int(pid),))
        st.dataframe(df, use_container_width=True)
        total_excl = float(df["amount_excl"].sum()) if not df.empty else 0.0
        total_vat = float((df["amount_excl"] * (df["vat_percent"]/100)).sum()) if not df.empty else 0.0
        total_incl = total_excl + total_vat
        a, b, c = st.columns(3)
        a.metric("Total (excl)", f"{cfg.get('currency','ZAR')} {total_excl:,.2f}")
        b.metric("VAT", f"{cfg.get('currency','ZAR')} {total_vat:,.2f}")
        c.metric("Total (incl)", f"{cfg.get('currency','ZAR')} {total_incl:,.2f}")
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"),
                           file_name=f"project_{pid}_purchases.csv", mime="text/csv")

# --------------- Settings ---------------
elif nav == "Settings":
    st.header("Settings")

    col1, col2 = st.columns([2,1])
    with col1:
        storage_root = st.text_input("Default Storage Root", cfg.get("storage_root", DEFAULT_STORAGE_ROOT))
        currency = st.selectbox("Currency", ["ZAR","USD","EUR","GBP"], index=["ZAR","USD","EUR","GBP"].index(cfg.get("currency","ZAR")))
        vat_default = st.number_input("Default VAT %", min_value=0.0, max_value=100.0, value=float(cfg.get("vat_percent", 15.0)))
    with col2:
        logo_up = st.file_uploader("Brand Logo (PNG/JPG)", type=["png","jpg","jpeg"])
        if st.button("Save Logo") and logo_up is not None:
            ext = os.path.splitext(logo_up.name)[1].lower()
            logo_path = os.path.join(ASSETS_DIR, f"brand_logo{ext}")
            with open(logo_path, "wb") as f:
                f.write(logo_up.getbuffer())
            cfg["brand_logo"] = logo_path
            save_cfg(cfg)
            st.success(f"Logo saved: {logo_path}")
            st.experimental_rerun()

    if st.button("Save Settings"):
        cfg["storage_root"] = storage_root.strip() or DEFAULT_STORAGE_ROOT
        cfg["currency"] = currency
        cfg["vat_percent"] = float(vat_default)
        save_cfg(cfg)
        st.success("Settings saved.")

    st.divider()
    st.subheader("Snapshots")
    if st.button("Create Full ZIP Snapshot"):
        out = snapshot_zip()
        with open(out, "rb") as f:
            st.download_button("Download Snapshot", f.read(), file_name=os.path.basename(out), mime="application/zip")
    snaps = [f for f in os.listdir(BACKUPS_DIR) if f.endswith(".zip")]
    if snaps:
        st.caption("Recent:")
        for s in sorted(snaps, reverse=True)[:10]:
            p = os.path.join(BACKUPS_DIR, s)
            with open(p, "rb") as f:
                st.download_button(s, f.read(), file_name=s, mime="application/zip", key=f"dl_snap_{s}")
