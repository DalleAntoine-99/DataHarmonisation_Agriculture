"""
Formatting layer — AGROVOC
Lit le JSON brut fetchés depuis l'API FAO et produit un fichier Parquet normalisé
dans datalake/formatted/agrovoc/concepts.parquet
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Chemins ───────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).resolve().parents[2]
RAW_DIR       = PROJECT_ROOT / "datalake/raw/agrovoc"
TODAY         = datetime.now(timezone.utc).strftime("%Y%m%d")
FORMATTED_DIR = PROJECT_ROOT / f"datalake/formatted/agrovoc/concepts/{TODAY}"
OUTPUT_PATH   = FORMATTED_DIR / "part-00001.parquet"

def find_latest_json() -> Path:
    """Trouve le fichier JSON le plus récent dans datalake/raw/agrovoc/"""
    json_files = sorted(RAW_DIR.rglob("agrovoc_dephy_terms.json"), key=lambda p: p.stat().st_mtime)
    if not json_files:
        print("Aucun fichier agrovoc_dephy_terms.json trouvé dans datalake/raw/agrovoc/", file=sys.stderr)
        sys.exit(2)
    latest = json_files[-1]
    print(f"Fichier JSON trouvé: {latest}")
    return latest

def parse_raw_json(raw_path: Path) -> pd.DataFrame:
    """Parse le JSON brut AGROVOC en DataFrame."""
    with raw_path.open(encoding="utf-8") as f:
        raw = json.load(f)

    rows = []
    for dephy_term, content in raw.items():
        # le JSON peut avoir 2 formats selon la version du script
        # format A: { "matched_query": "...", "results": [...] }
        # format B: [result1, result2, ...]
        if isinstance(content, dict) and "results" in content:
            matched_query = content.get("matched_query", dephy_term)
            results       = content.get("results", [])
        elif isinstance(content, list):
            matched_query = dephy_term
            results       = content
        else:
            continue

        for r in results:
            rows.append({
                "dephy_term":    dephy_term.strip(),
                "matched_query": matched_query.strip(),
                "agrovoc_uri":   r.get("uri", ""),
                "agrovoc_label": r.get("prefLabel", ""),
                "agrovoc_id":    r.get("localname", ""),
                "lang":          r.get("lang", "en"),
                "vocab":         r.get("vocab", "agrovoc"),
            })

    return pd.DataFrame(rows)

def normalize(df: pd.DataFrame, raw_path: Path) -> pd.DataFrame:
    # 1) Nettoyer les valeurs texte
    for col in ["dephy_term", "matched_query", "agrovoc_label"]:
        if col in df.columns:
            df[col] = df[col].str.strip()

    # 2) Normaliser dephy_term en minuscules
    df["dephy_term_normalized"] = df["dephy_term"].str.lower().str.strip()

    # 3) Extraire l'ID numérique AGROVOC depuis l'URI
    # ex: http://aims.fao.org/aos/agrovoc/c_918 → c_918
    df["agrovoc_concept_id"] = df["agrovoc_uri"].str.extract(r"(c_\d+)$")

    # 4) Ajouter les métadonnées de traçabilité (UTC)
    now_utc = datetime.now(timezone.utc).isoformat()
    df["source_file"]  = raw_path.name
    df["source_dir"]   = str(raw_path.parent.name)
    df["formatted_at"] = now_utc
    df["data_layer"]   = "formatted"

    # 5) Supprimer les doublons (même terme DEPHY + même concept AGROVOC)
    df = df.drop_duplicates(subset=["dephy_term", "agrovoc_uri"])

    # 6) Reset index propre
    df = df.reset_index(drop=True)

    return df

def main():
    # Trouver le JSON brut le plus récent
    raw_path = find_latest_json()

    print("🔄 Parsing du JSON brut AGROVOC ...")
    df = parse_raw_json(raw_path)
    print(f"   → {len(df)} correspondances brutes trouvées")

    if df.empty:
        print("⚠️  Aucune donnée à normaliser.", file=sys.stderr)
        sys.exit(0)

    print("🔄 Normalisation en cours ...")
    df_clean = normalize(df, raw_path)
    print(f"   → {len(df_clean)} lignes après dédoublonnage")
    print(f"   → {df_clean['dephy_term'].nunique()} termes DEPHY uniques couverts")
    print(f"   → Colonnes: {df_clean.columns.tolist()}")

    # Créer le dossier formatted si besoin
    FORMATTED_DIR.mkdir(parents=True, exist_ok=True)

    # Sauvegarder en Parquet
    df_clean.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")
    print(f"\n✅ Parquet sauvegardé: {OUTPUT_PATH}")
    print(f"   → Taille: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")

    # Aperçu rapide
    print(f"\n📊 Aperçu des 5 premières lignes:")
    print(df_clean[["dephy_term", "matched_query", "agrovoc_label", "agrovoc_concept_id"]].head(5).to_string())

if __name__ == "__main__":
    main()