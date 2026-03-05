"""
Export layer — DuckDB mart_alignments → Parquet dans le datalake
Source : Data_Harmo_dbt/dev.duckdb → table mart_alignments
Cible  : datalake/enriched/biocontrol/alignments/<run_date>/part-00001.parquet
"""
import sys
from pathlib import Path

import duckdb

# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # Data_Harmonisation/
DUCKDB_PATH  = PROJECT_ROOT / "Data_Harmo_dbt" / "dev.duckdb"
TABLE_NAME   = "mart_alignments"

# =============================================================================
# MAIN
# =============================================================================

def main():
    # ── Argument : run_date (YYYYMMDD) ────────────────────────────────────
    if len(sys.argv) < 2:
        print("Usage: export_parquet.py <run_date>  (ex: 20260227)")
        sys.exit(1)

    run_date = sys.argv[1]

    # ── Chemin de sortie ──────────────────────────────────────────────────
    output_dir = PROJECT_ROOT / "datalake" / "enriched" / "biocontrol" / "alignments" / run_date
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-00001.parquet"

    # ── Connexion DuckDB (read-only) ─────────────────────────────────────
    if not DUCKDB_PATH.exists():
        print(f"Base DuckDB introuvable : {DUCKDB_PATH}")
        sys.exit(2)

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # ── Vérifier que la table existe ─────────────────────────────────────
    tables = [row[0] for row in con.sql("SHOW TABLES").fetchall()]
    if TABLE_NAME not in tables:
        print(f"Table '{TABLE_NAME}' introuvable. Tables disponibles : {tables}")
        con.close()
        sys.exit(2)

    # ── Export ────────────────────────────────────────────────────────────
    df = con.sql(f"SELECT * FROM {TABLE_NAME}").df()
    con.close()

    print(f"Export de '{TABLE_NAME}' : {len(df)} lignes, {len(df.columns)} colonnes")

    # Convertir les timestamps timezone-aware en strings pour compatibilité Parquet
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].astype(str)

    df.to_parquet(output_path, index=False)

    size_kb = output_path.stat().st_size / 1024
    print(f"Parquet sauvegarde : {output_path}")
    print(f"  -> {len(df)} lignes, {size_kb:.1f} KB")


if __name__ == "__main__":
    main()