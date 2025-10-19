from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import duckdb
import pandas as pd
import os

DB_PATH = os.path.abspath("\data\coin_data.duckdb")
UMAP_PLOT = os.path.abspath("\data\umap_plot.png")
FRONTEND_DIR = os.path.abspath("\frontend")

app = FastAPI(title="CMC Analytics API", version="0.1")

# Enable CORS for local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static assets (JS, CSS, etc.)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

def get_conn():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DuckDB not found: {DB_PATH}")
    return duckdb.connect(DB_PATH)

# -------------------------
# Serve index.html (like Flask render_template)
# -------------------------
@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    index_file = os.path.join(FRONTEND_DIR, "index.html")
    if not os.path.exists(index_file):
        raise HTTPException(status_code=404, detail="index.html not found")
    with open(index_file, "r", encoding="utf-8") as f:
        return f.read()

# -------------------------
# API Endpoints
# -------------------------
@app.get("/coins")
def coins(limit: int = 100, offset: int = 0):
    try:
        con = get_conn()
        q = "SELECT * FROM coins ORDER BY fetch_time DESC LIMIT ? OFFSET ?"
        df = con.execute(q, [limit, offset]).df()
        con.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(df), "rows": df.to_dict(orient="records")}

@app.get("/analytics/embeddings")
def embeddings(limit: int = 500):
    try:
        con = get_conn()
        tables = con.execute("SHOW TABLES").df()["name"].tolist()
        if "coins_embeddings" in tables:
            df = con.execute("""
                SELECT id, name, symbol, pca_1, pca_2, umap_1, umap_2, cmc_rank
                FROM coins_embeddings ORDER BY cmc_rank LIMIT ?
            """, [limit]).df()
        else:
            df = con.execute("""
                SELECT id, name, symbol, cmc_rank
                FROM coins ORDER BY cmc_rank LIMIT ?
            """, [limit]).df()
        con.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(df), "embeddings": df.to_dict(orient="records")}

@app.get("/plot")
def plot():
    if not os.path.exists(UMAP_PLOT):
        raise HTTPException(status_code=404, detail=f"Plot not found: {UMAP_PLOT}")
    return FileResponse(UMAP_PLOT, media_type="image/png", filename="umap_plot.png")
