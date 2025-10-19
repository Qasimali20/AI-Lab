"""
Perform PCA and UMAP on cleaned cryptocurrency data and visualize results.
"""

import duckdb
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import umap
import matplotlib.pyplot as plt
import os

DB_PATH = r"C:\Users\Administrator\Desktop\CMC-DB\data\coin_data.duckdb"
PLOT_PATH = "data/umap_plot.png"

def run_pca_umap():
    # Connect and load data
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM coins").df()
    con.close()
    print(f"🔹 Loaded {len(df)} rows from DuckDB")

    # Select numeric features for analysis
    features = ["price", "market_cap", "volume_24h",
                "percent_change_1h", "percent_change_24h", "percent_change_7d"]
    df = df.dropna(subset=features)
    X = df[features].values

    # Scale data
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # --- PCA ---
    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(X_scaled)
    df["pca_1"] = pca_result[:, 0]
    df["pca_2"] = pca_result[:, 1]
    print(f"✅ PCA explained variance: {pca.explained_variance_ratio_.sum():.2%}")

    # --- UMAP ---
    reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, random_state=42)
    umap_result = reducer.fit_transform(X_scaled)
    df["umap_1"] = umap_result[:, 0]
    df["umap_2"] = umap_result[:, 1]
    print("✅ UMAP embedding completed")

    # --- Visualization ---
    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(df["umap_1"], df["umap_2"],
                          c=df["cmc_rank"], cmap="plasma", s=25, alpha=0.8)
    plt.colorbar(scatter, label="CMC Rank")
    plt.title("UMAP projection of Cryptocurrencies", fontsize=14)
    plt.xlabel("UMAP Dimension 1")
    plt.ylabel("UMAP Dimension 2")
    plt.tight_layout()

    os.makedirs(os.path.dirname(PLOT_PATH), exist_ok=True)
    plt.savefig(PLOT_PATH, dpi=300)
    plt.show()
    print(f"💾 Saved UMAP plot to {PLOT_PATH}")

    # Optional: Save PCA + UMAP results back to DuckDB
    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS coins_embeddings")
    con.execute("CREATE TABLE coins_embeddings AS SELECT * FROM df")
    con.close()
    print("💾 Saved PCA + UMAP embeddings to DuckDB (table: coins_embeddings)")

if __name__ == "__main__":
    run_pca_umap()
