# OppWorks Procurement App — v0.4-pipeline
# Author: ChatGPT (Developer Hat)
# Storage: uses OPP_DATA_ROOT (e.g., /persist on Render)
# Snapshots: versioned documents + full ZIP backups
# New in v0.4:
# - Sidebar: Dashboard, Purchases + stage views, Suppliers, Projects, Documents, Reports,
#            Approvers & Limits (moved), Settings
# - Suppliers: description + “Generate Supplier PDF” saved to master folder
# - Projects: extended fields (GL-Code, Operation Unit, Cost Centre, Misc, Partner, Approver)
#             + “Generate Project PDF” saved to master folder
# - Stage pages (Quote Received / Requisition Sent / Order Sent / Delivery / Invoice Signed / Sent for Receipting)
#   for tracking each order; quick “Mark date today” and upload/paste document for that stage
# - Documents: simple tracker + “missing steps” report

import os, io, json, sqlite3, zipfile, shutil
from datetime import datetime, date
from typing import Optional, Tuple

import pandas as pd
from fpdf import FPDF
import streamlit as st

APP_VERSION = "v0.4-pipeline"

# ---------- Paths ----------
ROOT = os.environ.get("OPP_DATA_ROOT", os.path.abspath("."))
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "procurement.db")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
ASSETS_DIR = os.path.join(ROOT, "assets")
DEFAULT_STORAGE_ROOT = os.path.join(ROOT, "OppWorks_Procurement")
BACKUPS_DIR = os.path.join(ROOT, "backups")
SUPPLIER_SHEETS_DIR = os.path.join(DEFAULT_STORAGE_ROOT, "_Suppliers")
PROJECT_SHEETS_DIR = os.path.join(DEFAULT_STORAGE_ROOT, "_ProjectSheets")

for p in [DATA_DIR, ASSETS_DIR, DEFAULT_STORAGE_ROOT, BACKUPS_DIR, SUPPLIER_SHEETS_DIR, PROJECT_SHEETS_DIR]:
    os.makedirs(p, exist_ok=True)

# ---------- DB ----------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables():
    c = get_conn(); cur = c.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS suppliers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        company TEXT, email TEXT, phone TEXT,
        desc TEXT,                -- NEW: description of goods/services
        created_at TEXT NOT NULL
    );""")
    # safe ALTERs
    try: cur.execute("ALTER TABLE suppliers ADD COLUMN desc TEXT")
    except: pass

    cur.execute("""CREATE TABLE IF NOT EXISTS approvers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        role TEXT,
        limit_amount REAL NOT NULL,
        created_at TEXT NOT NULL
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS projects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        location TEXT,
        capex_code TEXT,
        cost_category TEXT,
        root_folder TEXT NOT NULL,
        gl_code TEXT,             -- NEW
        operation_unit TEXT,      -- NEW
        cost_centre TEXT,         -- NEW
        miscellaneous TEXT,       -- NEW
        partner TEXT,             -- NEW
        approver_id INTEGER,      -- NEW (FK to approvers)
        created_at TEXT NOT NULL
    );""")
    # safe ALTERs
    for col in ["gl_code","operation_unit","cost_centre","miscellaneous","partner","approver_id"]:
        try: cur.execute(f"ALTER TABLE projects ADD COLUMN {col} TEXT")
        except: pass
    try: cur.execute("ALTER TABLE projects ADD COLUMN approver_id INTEGER")
    except: pass

    cur.execute("""CREATE TABLE IF NOT EXISTS purchases(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        supplier_id INTEGER NOT NULL,
        item_description TEXT,
        category TEXT, amount_excl_vat REAL DEFAULT 0, vat_percent REAL DEFAULT 15,
        payment_terms TEXT,
        rfq_sent_date TEXT,
        quote_received_date TEXT,
        requisition_requested_date TEXT,
        order_sent_date TEXT,
        delivered_date TEXT,
        invoice_signed_date TEXT,
        receipting_sent_date TEXT,
        status TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(project_id) REFERENCES projects(id),
        FOREIGN KEY(supplier_id) REFERENCES suppliers(id)
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS documents(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        purchase_id INTEGER NOT NULL,
        doc_type TEXT NOT NULL,    -- Quote / Order / Delivery / Invoice
        filename TEXT NOT NULL,
        saved_path TEXT NOT NULL,
        uploaded_at TEXT NOT NULL,
        version INTEGER DEFAULT 1,
        is_current INTEGER DEFAULT 1
    );""")
    # safe ALTERs
    for col_def in ["version INTEGER DEFAULT 1","is_current INTEGER DEFAULT 1"]:
        try: cur.execute(f"ALTER TABLE documents ADD COLUMN {col_def}")
        except: pass

    c.commit(); c.close()

# ---------- Helpers ----------
STATUS_ORDER = [
    ("rfq_sent_date", "RFQ Sent"),
    ("quote_received_date", "Quote Received"),
    ("requisition_requested_date", "Requisition Requested"),
    ("order_sent_date", "Order Sent"),
    ("delivered_date", "Delivered"),
    ("invoice_signed_date", "Invoice Signed"),
    ("receipting_sent_date", "Sent for Receipting"),
]

def month_text_date(dt: date) -> str:
    return dt.strftime("%d %B %Y")

def load_config() -> dict:
    cfg = {"storage_root": DEFAULT_STORAGE_ROOT, "brand_logo_path": "", "currency":"ZAR", "vat_percent":15.0}
    if os.path.exists(CONFIG_PATH):
        try:
            cfg.update(json.load(open(CONFIG_PATH, "r", encoding="utf-8")))
        except: pass
    return cfg

def save_config(cfg: dict):
    json.dump(cfg, open(CONFIG_PATH, "w", encoding="utf-8"), indent=2)

def ensure_project_folders(root_folder: str):
    for sub in ["Quote","Order","Delivery","Invoice"]:
        os.makedirs(os.path.join(root_folder, sub), exist_ok=True)

def read_df(q:str, params:Tuple=()):
    conn=get_conn(); df=pd.read_sql_query(q, conn, params=params); conn.close(); return df

def exec_sql(q:str, params:Tuple=()):
    conn=get_conn(); cur=conn.cursor(); cur.execute(q, params); conn.commit(); lid=cur.lastrowid; conn.close(); return lid

def derive_status(row:dict)->str:
    last = "Not Started"
    for col,label in STATUS_ORDER:
        if row.get(col): last = label
    return last

def get_purchase_dict(pid:int)->dict:
    conn=get_conn(); cur=conn.cursor()
    cur.execute("""SELECT rfq_sent_date,quote_received_date,requisition_requested_date,
                   order_sent_date,delivered_date,invoice_signed_date,receipting_sent_date
                   FROM purchases WHERE id=?""",(pid,))
    r = cur.fetchone()
    conn.close()
    if not r: return {}
    cols = ["rfq_sent_date","quote_received_date","requisition_requested_date","order_sent_date","delivered_date","invoice_signed_date","receipting_sent_date"]
    return {k:r[i] for i,k in enumerate(cols)}

def update_stage_date(pid:int, col:str, dt:date):
    row = get_purchase_dict(pid)
    row[col] = month_text_date(dt)
    status = derive_status(row)
    exec_sql(f"UPDATE purchases SET {col}=?, status=? WHERE id=?", (row[col], status, pid))
    return status

def next_version(purchase_id:int, doc_type:str)->int:
    conn=get_conn(); cur=conn.cursor()
    cur.execute("SELECT COALESCE(MAX(version),0) FROM documents WHERE purchase_id=? AND doc_type=?",(purchase_id,doc_type))
    v=(cur.fetchone()[0] or 0)+1; conn.close(); return v

def versioned_name(filename:str, v:int)->str:
    base,ext=os.path.splitext(filename); return f"{base}_v{v}{ext}"

def save_file(uploaded_file, folder:str, v:int)->Tuple[str,str]:
    os.makedirs(folder, exist_ok=True)
    fname=versioned_name(uploaded_file.name, v)
    dest=os.path.join(folder, fname)
    i=1
    while os.path.exists(dest):
        b,e=os.path.splitext(fname); fname=f"{b}_{i}{e}"; dest=os.path.join(folder,fname); i+=1
    with open(dest,"wb") as f: f.write(uploaded_file.getbuffer())
    return fname, dest

def save_text_pdf(text:str, folder:str, logo:str, v:int)->Tuple[str,str]:
    os.makedirs(folder, exist_ok=True)
    fname=f"Pasted_v{v}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    dest=os.path.join(folder, fname)
    pdf=FPDF(); pdf.set_auto_page_break(True,15); pdf.add_page()
    if logo and os.path.exists(logo):
        try: pdf.image(logo, x=10, y=8, w=30)
        except: pass
    pdf.set_font("Arial", size=12); pdf.cell(0,10,"OppWorks — Procurement Document",ln=True,align="R"); pdf.ln(8)
    pdf.set_font("Arial", size=11)
    for line in text.splitlines(): pdf.multi_cell(0,7,line)
    pdf.output(dest)
    return fname,dest

def snapshot_copy(src:str, pid:int, dtype:str, v:int):
    dest_dir=os.path.join(BACKUPS_DIR,"documents",f"purchase_{pid}",dtype)
    os.makedirs(dest_dir, exist_ok=True)
    try: shutil.copy2(src, os.path.join(dest_dir, f"v{v}_"+os.path.basename(src)))
    except: pass

def full_zip_snapshot()->str:
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    out=os.path.join(BACKUPS_DIR, f"oppworks_snapshot_{ts}.zip")
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for root_dir in [DATA_DIR, DEFAULT_STORAGE_ROOT]:
            for folder,_,files in os.walk(root_dir):
                for f in files:
                    full=os.path.join(folder,f); rel=os.path.relpath(full, ROOT); z.write(full, rel)
    return out

# ---------- PDF sheets ----------
def pdf_supplier_sheet(row, logo:str)->str:
    os.makedirs(SUPPLIER_SHEETS_DIR, exist_ok=True)
    fname=f"Supplier_{row['company'] or row['name']}_{datetime.now().strftime('%Y%m%d')}.pdf".replace(" ","_")
    path=os.path.join(SUPPLIER_SHEETS_DIR, fname)
    pdf=FPDF(); pdf.add_page(); pdf.set_auto_page_break(True,15)
    if logo and os.path.exists(logo):
        try: pdf.image(logo, x=10, y=8, w=30)
        except: pass
    pdf.set_font("Arial","B",14); pdf.cell(0,10,"Supplier Summary",ln=True,align="R"); pdf.ln(6)
    pdf.set_font("Arial", size=11)
    def line(k,v): pdf.cell(45,8,f"{k}:",0); pdf.multi_cell(0,8,str(v or ""))
    line("Company", row["company"]); line("Contact", row["name"])
    line("Email", row["email"]); line("Phone", row["phone"])
    pdf.ln(2); pdf.set_font("Arial","B",12); pdf.cell(0,8,"Goods/Services",ln=True)
    pdf.set_font("Arial", size=11); pdf.multi_cell(0,7,row["desc"] or "")
    pdf.ln(4); pdf.set_font("Arial", size=10); pdf.cell(0,8,f"Generated: {month_text_date(date.today())}",ln=True)
    pdf.output(path); return path

def pdf_project_sheet(proj, approver, logo:str)->str:
    os.makedirs(PROJECT_SHEETS_DIR, exist_ok=True)
    fname=f"Project_{proj['name']}_{datetime.now().strftime('%Y%m%d')}.pdf".replace(" ","_")
    path=os.path.join(PROJECT_SHEETS_DIR, fname)
    pdf=FPDF(); pdf.add_page(); pdf.set_auto_page_break(True,15)
    if logo and os.path.exists(logo):
        try: pdf.image(logo, x=10, y=8, w=30)
        except: pass
    pdf.set_font("Arial","B",14); pdf.cell(0,10,"Project Sheet",ln=True,align="R"); pdf.ln(6)
    pdf.set_font("Arial", size=11)
    def line(k,v): pdf.cell(45,8,f"{k}",0); pdf.cell(0,8,f"—  {v or ''}",ln=True)
    line("Project name", proj["name"])
    line("Location", proj["location"])
    line("GL-Code", proj["gl_code"])
    line("Operation Unit", proj["operation_unit"])
    line("Cost Centre", proj["cost_centre"])
    line("Miscellaneous", proj["miscellaneous"])
    line("Partner", proj["partner"])
    appr_text = f"{approver['name']} ({approver['role']}, limit {approver['limit_amount']})" if approver else ""
    line("Approver", appr_text)
    pdf.ln(4); pdf.set_font("Arial", size=10); pdf.cell(0,8,f"Generated: {month_text_date(date.today())}",ln=True)
    pdf.output(path); return path

# ---------- App ----------
st.set_page_config(page_title=f"OppWorks Procurement {APP_VERSION}", page_icon="📦", layout="wide")
ensure_tables()
cfg = load_config()

with st.sidebar:
    st.title("📦 OppWorks Procurement")
    st.caption(APP_VERSION)
    if cfg.get("brand_logo_path") and os.path.exists(cfg["brand_logo_path"]):
        st.image(cfg["brand_logo_path"], use_container_width=True)
    nav = st.radio("Navigation", [
        "Dashboard",
        "Purchases",
        "Quote Received",
        "Requisition Sent",
        "Order Sent",
        "Delivery / Invoice",
        "Invoice Signed",
        "Sent for Receipting",
        "Suppliers",
        "Projects",
        "Documents",
        "Reports",
        "Approvers & Limits",
        "Settings",
    ], index=0)

# ---------- Dashboard ----------
if nav == "Dashboard":
    st.header("Dashboard")
    c1,c2,c3,c4 = st.columns(4)
    n_sup = read_df("SELECT COUNT(*) n FROM suppliers")["n"][0]
    n_prj = read_df("SELECT COUNT(*) n FROM projects")["n"][0]
    n_pur = read_df("SELECT COUNT(*) n FROM purchases")["n"][0]
    total = read_df("SELECT COALESCE(SUM(amount_excl_vat),0) amt FROM purchases")["amt"][0]
    c1.metric("Suppliers", n_sup); c2.metric("Projects", n_prj)
    c3.metric("Purchases", n_pur); c4.metric("Spend (excl VAT)", f"{cfg.get('currency','ZAR')} {total:,.2f}")

    st.subheader("Pipeline")
    dfp = read_df("""SELECT status, COUNT(*) n FROM purchases GROUP BY status""")
    st.dataframe(dfp, use_container_width=True)

    st.subheader("Missing Steps")
    dfm = read_df("""SELECT p.id, pj.name project, s.company supplier, p.rfq_sent_date, p.quote_received_date,
                     p.requisition_requested_date, p.order_sent_date, p.delivered_date, p.invoice_signed_date, p.receipting_sent_date
                     FROM purchases p
                     JOIN projects pj ON pj.id=p.project_id
                     JOIN suppliers s ON s.id=p.supplier_id
                     ORDER BY p.id DESC""")
    for col,_ in STATUS_ORDER:
        if col in dfm.columns:
            dfm[f"{col}_missing"] = dfm[col].isna() | (dfm[col]=="") 
    st.dataframe(dfm[[c for c in dfm.columns if "missing" in c] + ["id","project","supplier"]], use_container_width=True)

# ---------- Suppliers ----------
elif nav == "Suppliers":
    st.header("Suppliers")
    with st.form("add_sup"):
        c1,c2 = st.columns(2)
        with c1:
            name = st.text_input("Contact Name *")
            company = st.text_input("Company")
        with c2:
            email = st.text_input("Email")
            phone = st.text_input("Phone")
        desc = st.text_area("Goods/Services description")
        go = st.form_submit_button("Save Supplier")
        if go and name:
            exec_sql("INSERT INTO suppliers(name,company,email,phone,desc,created_at) VALUES(?,?,?,?,?,?)",
                     (name.strip(), company.strip(), email.strip(), phone.strip(), desc.strip(), month_text_date(date.today())))
            st.success("Supplier saved.")
    st.subheader("All Suppliers")
    dfs = read_df("SELECT id, company, name, email, phone, desc, created_at FROM suppliers ORDER BY id DESC")
    st.dataframe(dfs, use_container_width=True)

    st.subheader("Generate Supplier PDF")
    if not dfs.empty:
        sid = st.selectbox("Supplier", options=dfs["id"].tolist(),
                           format_func=lambda i: f"{dfs.set_index('id').loc[i,'company'] or ''} — {dfs.set_index('id').loc[i,'name']}")
        if st.button("Create PDF in master folder"):
            row = dfs.set_index("id").loc[int(sid)]
            out = pdf_supplier_sheet(row, cfg.get("brand_logo_path"))
            st.success(f"Saved: {out}")
            with open(out, "rb") as f:
                st.download_button("Download now", data=f.read(), file_name=os.path.basename(out), mime="application/pdf")

# ---------- Projects ----------
elif nav == "Projects":
    st.header("Projects")
    df_ap = read_df("SELECT id, name, role, limit_amount FROM approvers ORDER BY name")
    with st.form("add_prj"):
        st.subheader("Add Project")
        left, right = st.columns([1,2])
        with left:
            st.write("Project name")
            st.write("Location")
            st.write("GL-Code")
            st.write("Operation Unit")
            st.write("Cost Centre")
            st.write("Miscellaneous")
            st.write("Partner")
            st.write("Approver")
        with right:
            name = st.text_input(" ", key="prj_name")
            location = st.selectbox(" ", ["Boksburg","Piet Retief","Ugie","Other"], index=0, key="prj_loc")
            gl_code = st.text_input(" ", key="prj_gl")
            operation_unit = st.text_input(" ", key="prj_opu")
            cost_centre = st.text_input(" ", key="prj_cc")
            miscellaneous = st.text_input(" ", key="prj_misc")
            partner = st.text_input(" ", key="prj_partner")
            appr_id = st.selectbox(" ", options=[None]+df_ap["id"].tolist(),
                                   format_func=lambda i: "—" if i is None else f"{df_ap.set_index('id').loc[i,'name']} ({df_ap.set_index('id').loc[i,'role']})")
        c1,c2 = st.columns(2)
        with c1:
            capex_code = st.text_input("CAPEX / Cost Code (optional)")
            cost_category = st.selectbox("Cost Category", ["Capex","Goods","Services"], index=0)
        with c2:
            prj_root_override = st.text_input("Project Root Folder (leave blank for default)", value="")
        go = st.form_submit_button("Create Project & Folders")
        if go and name:
            base = prj_root_override.strip() or cfg.get("storage_root", DEFAULT_STORAGE_ROOT)
            prj_root = os.path.join(base, name.strip())
            os.makedirs(prj_root, exist_ok=True); ensure_project_folders(prj_root)
            exec_sql("""INSERT INTO projects(name,location,capex_code,cost_category,root_folder,gl_code,operation_unit,cost_centre,miscellaneous,partner,approver_id,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (name.strip(), location, capex_code.strip(), cost_category, prj_root,
                      gl_code.strip(), operation_unit.strip(), cost_centre.strip(), miscellaneous.strip(),
                      partner.strip(), int(appr_id) if appr_id else None, month_text_date(date.today())))
            st.success(f"Project created at: {prj_root}")

    st.subheader("Project Sheet (PDF)")
    dfp = read_df("SELECT * FROM projects ORDER BY id DESC")
    st.dataframe(dfp[["id","name","location","gl_code","operation_unit","cost_centre","partner","approver_id","root_folder","created_at"]], use_container_width=True)
    if not dfp.empty:
        pid = st.selectbox("Choose a project", options=dfp["id"].tolist(),
                           format_func=lambda i: dfp.set_index("id").loc[i,"name"])
        if st.button("Generate Project PDF in master folder"):
            proj = dict(dfp.set_index("id").loc[int(pid)])
            appr = None
            if proj.get("approver_id"):
                apdf = read_df("SELECT * FROM approvers WHERE id=?", (int(proj["approver_id"]),))
                appr = dict(apdf.iloc[0]) if not apdf.empty else None
            out = pdf_project_sheet(proj, appr, cfg.get("brand_logo_path"))
            st.success(f"Saved: {out}")
            with open(out,"rb") as f:
                st.download_button("Download now", data=f.read(), file_name=os.path.basename(out), mime="application/pdf")

# ---------- Purchases (create + overview) ----------
elif nav == "Purchases":
    st.header("Purchases / RFQs")
    df_projects = read_df("SELECT id, name FROM projects ORDER BY name")
    df_suppliers = read_df("SELECT id, company || COALESCE(' — '||name,'') AS label FROM suppliers ORDER BY company, name")
    if df_projects.empty or df_suppliers.empty:
        st.warning("Add at least one Project and one Supplier first.")
    else:
        with st.form("add_purchase"):
            c1,c2 = st.columns([2,1])
            with c1:
                project_id = st.selectbox("Project *", df_projects["id"].tolist(), format_func=lambda i: df_projects.set_index("id").loc[i,"name"])
                supplier_id = st.selectbox("Supplier *", df_suppliers["id"].tolist(), format_func=lambda i: df_suppliers.set_index("id").loc[i,"label"])
                item_description = st.text_area("Item / Service Description")
            with c2:
                category = st.selectbox("Category", ["Goods","Services"], index=0)
                amount_excl = st.number_input("Amount (excl VAT)", min_value=0.0, step=100.0)
                vat_percent = st.number_input("VAT %", min_value=0.0, max_value=100.0, value=float(cfg.get("vat_percent",15.0)), step=0.5)
                payment_terms = st.text_input("Payment Terms")
            st.markdown("---")
            t1,t2,t3 = st.columns(3)
            with t1:
                rfq = st.date_input("RFQ Sent", value=date.today())
                quote = st.date_input("Quote Received")
            with t2:
                req = st.date_input("Requisition Requested")
                order = st.date_input("Order Sent")
            with t3:
                delivered = st.date_input("Delivered")
                inv_signed = st.date_input("Invoice Signed")
                recpt = st.date_input("Sent for Receipting")
            go = st.form_submit_button("Save Purchase")
            if go:
                row = {
                    "rfq_sent_date": month_text_date(rfq) if rfq else None,
                    "quote_received_date": month_text_date(quote) if quote else None,
                    "requisition_requested_date": month_text_date(req) if req else None,
                    "order_sent_date": month_text_date(order) if order else None,
                    "delivered_date": month_text_date(delivered) if delivered else None,
                    "invoice_signed_date": month_text_date(inv_signed) if inv_signed else None,
                    "receipting_sent_date": month_text_date(recpt) if recpt else None,
                }
                status = derive_status(row)
                exec_sql("""INSERT INTO purchases(project_id,supplier_id,item_description,category,amount_excl_vat,vat_percent,payment_terms,
                        rfq_sent_date,quote_received_date,requisition_requested_date,order_sent_date,delivered_date,invoice_signed_date,receipting_sent_date,
                        status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (int(project_id), int(supplier_id), item_description.strip(), category, float(amount_excl), float(vat_percent), payment_terms.strip(),
                         row["rfq_sent_date"], row["quote_received_date"], row["requisition_requested_date"], row["order_sent_date"], row["delivered_date"], row["invoice_signed_date"], row["receipting_sent_date"],
                         status, month_text_date(date.today())))
                st.success(f"Purchase saved with status: {status}")

    st.subheader("All Purchases")
    dfp = read_df("""SELECT p.id, pj.name project, s.company supplier, p.item_description, p.category, p.amount_excl_vat, p.status, p.created_at
                     FROM purchases p JOIN projects pj ON pj.id=p.project_id JOIN suppliers s ON s.id=p.supplier_id
                     ORDER BY p.id DESC""")
    st.dataframe(dfp, use_container_width=True)

# ---------- Stage views ----------
def stage_page(title:str, stage_col:str, stage_label:str, default_doc_type:str="Quote", allow_doc_choice:bool=False):
    st.header(title)
    df = read_df("""SELECT p.id, pj.name project, s.company supplier, p.item_description, p.amount_excl_vat, p.status
                    FROM purchases p JOIN projects pj ON pj.id=p.project_id JOIN suppliers s ON s.id=p.supplier_id
                    WHERE p.status=? ORDER BY p.id DESC""", (stage_label,))
    st.subheader("Purchases at this stage")
    st.dataframe(df, use_container_width=True)
    all_df = read_df("""SELECT p.id, pj.name project, s.company supplier, pj.root_folder prj_root
                        FROM purchases p JOIN projects pj ON pj.id=p.project_id JOIN suppliers s ON s.id=p.supplier_id
                        ORDER BY p.id DESC""")
    if all_df.empty: return
    st.markdown("### Quick update")
    pid = st.selectbox("Purchase", options=all_df["id"].tolist(),
                       format_func=lambda i: f"#{i} — {all_df.set_index('id').loc[i,'project']} — {all_df.set_index('id').loc[i,'supplier']}")
    set_today = st.button("Mark date as today")
    dtype = default_doc_type
    if allow_doc_choice:
        dtype = st.selectbox("Document Type", ["Delivery","Invoice"], index=0)
    uploaded = st.file_uploader("Upload a file for this stage", key=f"up_{title}")
    pasted = st.text_area("Or paste text → PDF", key=f"tx_{title}")
    go = st.button("Save document")
    if set_today:
        new_status = update_stage_date(int(pid), stage_col, date.today())
        st.success(f"{stage_label} date saved. Status now: {new_status}")
    if go and (uploaded or pasted.strip()):
        row = all_df.set_index("id").loc[int(pid)]
        v = next_version(int(pid), dtype)
        dest = os.path.join(row["prj_root"], dtype)
        if uploaded:
            fname, fpath = save_file(uploaded, dest, v)
        else:
            fname, fpath = save_text_pdf(pasted, dest, cfg.get("brand_logo_path"), v)
        doc_id = exec_sql("INSERT INTO documents(purchase_id,doc_type,filename,saved_path,uploaded_at,version,is_current) VALUES(?,?,?,?,?,?,1)",
                          (int(pid), dtype, fname, fpath, month_text_date(date.today()), v))
        # mark previous non-current
        exec_sql("UPDATE documents SET is_current=0 WHERE purchase_id=? AND doc_type=? AND id<>?",
                 (int(pid), dtype, int(doc_id)))
        snapshot_copy(fpath, int(pid), dtype, v)
        st.success(f"Saved {dtype} v{v}: {fname}")

elif nav == "Quote Received":
    stage_page("Quote Received", "quote_received_date", "Quote Received", default_doc_type="Quote")

elif nav == "Requisition Sent":
    stage_page("Requisition Sent", "requisition_requested_date", "Requisition Requested", default_doc_type="Quote")

elif nav == "Order Sent":
    stage_page("Order Sent", "order_sent_date", "Order Sent", default_doc_type="Order")

elif nav == "Delivery / Invoice":
    stage_page("Delivery / Invoice", "delivered_date", "Delivered", default_doc_type="Delivery", allow_doc_choice=True)

elif nav == "Invoice Signed":
    stage_page("Invoice Signed", "invoice_signed_date", "Invoice Signed", default_doc_type="Invoice")

elif nav == "Sent for Receipting":
    stage_page("Sent for Receipting", "receipting_sent_date", "Sent for Receipting", default_doc_type="Invoice")

# ---------- Documents ----------
elif nav == "Documents":
    st.header("Documents tracker")
    dfp = read_df("""SELECT p.id, pj.name project, s.company supplier,
                     p.quote_received_date, p.requisition_requested_date, p.order_sent_date,
                     p.delivered_date, p.invoice_signed_date, p.receipting_sent_date
                     FROM purchases p JOIN projects pj ON pj.id=p.project_id JOIN suppliers s ON s.id=p.supplier_id
                     ORDER BY p.id DESC""")
    st.subheader("Per-purchase status dates")
    st.dataframe(dfp, use_container_width=True)

    dfd = read_df("""SELECT d.purchase_id, d.doc_type, MAX(d.version) as latest_version, SUM(CASE WHEN is_current=1 THEN 1 ELSE 0 END) as current
                     FROM documents d GROUP BY d.purchase_id, d.doc_type""")
    st.subheader("Documents uploaded (latest version per type)")
    st.dataframe(dfd, use_container_width=True)

# ---------- Reports ----------
elif nav == "Reports":
    st.header("Reports")
    df_prj = read_df("SELECT id, name FROM projects ORDER BY name")
    if df_prj.empty:
        st.warning("Add a project to view reports.")
    else:
        pid = st.selectbox("Project", options=df_prj["id"].tolist(),
                           format_func=lambda i: df_prj.set_index("id").loc[i,"name"])
        df = read_df("""SELECT p.id, s.company supplier, p.item_description, p.category,
                        p.amount_excl_vat, p.vat_percent, p.status
                        FROM purchases p JOIN suppliers s ON s.id=p.supplier_id
                        WHERE p.project_id=? ORDER BY p.id DESC""",(int(pid),))
        st.dataframe(df, use_container_width=True)
        total_excl = float(df["amount_excl_vat"].sum()) if not df.empty else 0.0
        total_vat  = float((df["amount_excl_vat"]*(df["vat_percent"]/100.0)).sum()) if not df.empty else 0.0
        total_incl = total_excl + total_vat
        c1,c2,c3 = st.columns(3)
        c1.metric("Total (excl)", f"{cfg.get('currency','ZAR')} {total_excl:,.2f}")
        c2.metric("VAT", f"{cfg.get('currency','ZAR')} {total_vat:,.2f}")
        c3.metric("Total (incl)", f"{cfg.get('currency','ZAR')} {total_incl:,.2f}")
        st.download_button("Download CSV", data=df.to_csv(index=False).encode("utf-8"),
                           file_name="project_purchases.csv", mime="text/csv")

# ---------- Approvers & Limits ----------
elif nav == "Approvers & Limits":
    st.header("Approvers & Limits")
    with st.form("add_app"):
        n = st.text_input("Name")
        r = st.text_input("Role")
        lim = st.number_input("Limit Amount (excl)", min_value=0.0, step=1000.0)
        go = st.form_submit_button("Add Approver")
        if go and n:
            exec_sql("INSERT INTO approvers(name,role,limit_amount,created_at) VALUES(?,?,?,?)",
                     (n.strip(), r.strip(), float(lim), month_text_date(date.today())))
            st.success("Approver saved.")
    dfa = read_df("SELECT id, name, role, limit_amount, created_at FROM approvers ORDER BY id DESC")
    st.dataframe(dfa, use_container_width=True)

# ---------- Settings ----------
elif nav == "Settings":
    st.header("Settings")
    c1,c2 = st.columns([2,1])
    with c1:
        storage_root = st.text_input("Default Storage Root", value=cfg.get("storage_root", DEFAULT_STORAGE_ROOT))
        currency = st.selectbox("Currency", ["ZAR","USD","EUR","GBP"], index=["ZAR","USD","EUR","GBP"].index(cfg.get("currency","ZAR")))
        vat = st.number_input("Default VAT %", min_value=0.0, max_value=100.0, step=0.5, value=float(cfg.get("vat_percent",15.0)))
    with c2:
        st.caption("Brand Logo (PNG/JPG)")
        logo_file = st.file_uploader("Upload logo", type=["png","jpg","jpeg"])
        if st.button("Save Logo") and logo_file is not None:
            ext=os.path.splitext(logo_file.name)[1].lower()
            path=os.path.join(ASSETS_DIR, f"brand_logo{ext}")
            with open(path,"wb") as f: f.write(logo_file.getbuffer())
            cfg["brand_logo_path"]=path; save_config(cfg); st.success(f"Logo saved: {path}"); st.experimental_rerun()

    if st.button("Save Settings"):
        cfg["storage_root"] = storage_root.strip() or DEFAULT_STORAGE_ROOT
        cfg["currency"] = currency
        cfg["vat_percent"] = float(vat)
        save_config(cfg); st.success("Settings saved.")

    st.divider()
    st.subheader("Snapshots & Backups")
    c1,c2 = st.columns(2)
    with c1:
        if st.button("Create Full ZIP Snapshot Now"):
            out = full_zip_snapshot()
            with open(out,"rb") as f:
                st.download_button("Download snapshot ZIP", data=f.read(), file_name=os.path.basename(out), mime="application/zip")
    with c2:
        snaps=[f for f in os.listdir(BACKUPS_DIR) if f.endswith(".zip")]; snaps.sort(reverse=True)
        if snaps:
            st.write("Recent snapshots:")
            for s in snaps[:10]:
                p=os.path.join(BACKUPS_DIR,s)
                with open(p,"rb") as f:
                    st.download_button(f"Download {s}", data=f.read(), file_name=s, mime="application/zip", key=f"dl_{s}")
        else:
            st.caption("No ZIP snapshots yet.")
