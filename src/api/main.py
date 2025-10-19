# src/api/main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
import os

DB_PATH = os.path.abspath(r"C:\Users\Administrator\Desktop\CMC-DB\data\coin_data.duckdb")
UMAP_PLOT = os.path.abspath(r"C:\Users\Administrator\Desktop\CMC-DB\data\umap_plot.png")

app = FastAPI(title="CMC Analytics API", version="0.1")

# Allow local dev origins; modify for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DuckDB not found: {DB_PATH}")
    return duckdb.connect(DB_PATH)

@app.get("/")
def root():
    return {"status": "ok", "service": "CMC Analytics API"}

@app.get("/coins")
def coins(limit: int = 100, offset: int = 0):
    """
    Return paginated cleaned coin rows from table `coins`.
    Example: /coins?limit=50&offset=0
    """
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
    """
    Return PCA + UMAP embeddings from coins_embeddings table (or compute fallback).
    """
    try:
        con = get_conn()
        # prefer embeddings table if exists
        tables = con.execute("SHOW TABLES").df()["name"].tolist()
        if "coins_embeddings" in tables:
            df = con.execute("SELECT id, name, symbol, pca_1, pca_2, umap_1, umap_2, cmc_rank FROM coins_embeddings ORDER BY cmc_rank LIMIT ?",
                             [limit]).df()
        else:
            # fallback: compute from coins (small dataset)
            df = con.execute("SELECT id, name, symbol, cmc_rank FROM coins ORDER BY cmc_rank LIMIT ?",
                             [limit]).df()
        con.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(df), "embeddings": df.to_dict(orient="records")}

@app.get("/plot")
def plot():
    """
    Return UMAP plot image (PNG). If not available, return 404.
    """
    if not os.path.exists(UMAP_PLOT):
        raise HTTPException(status_code=404, detail=f"Plot not found: {UMAP_PLOT}")
    return FileResponse(UMAP_PLOT, media_type="image/png", filename="umap_plot.png")
