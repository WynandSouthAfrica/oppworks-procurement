# OppWorks Procurement — Streamlit (v0.2-hosted)

Personal procurement tracker tailored to PG Bison workflow. Runs on Render with persistent storage.

## Features
- Suppliers, Projects, Purchases, Approvers, Documents (SQLite)
- Auto-create per-project folders: `Quote/Order/Delivery/Invoice`
- Paste text → PDF with logo, stored under the correct project subfolder
- Project reports with totals + PDF export
- Long-term persistence via `OPP_DATA_ROOT`

## Run locally
```bash
pip install -r requirements.txt
export OPP_DATA_ROOT=$(pwd)   # or any folder
streamlit run app.py
```

## Deploy to Render
1. Connect this repo.
2. Web Service → Python
3. Build: `pip install -r requirements.txt`
4. Start: `streamlit run app.py --server.port=$PORT --server.address=0.0.0.0`
5. Add Disk: name `oppworks-data`, mount `/persist`, size 10GB
6. Env var: `OPP_DATA_ROOT=/persist`
7. Deploy. Open the URL.

## Paths on Render
- DB: `/persist/data/procurement.db`
- Assets: `/persist/assets/brand_logo.png`
- Project files: `/persist/OppWorks_Procurement/<Project>/{Quote,Order,Delivery,Invoice}`
