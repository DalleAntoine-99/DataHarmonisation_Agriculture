"""
Indexing layer - Alignements DEPHY/AGROVOC vers Elasticsearch
Source : datalake/enriched/biocontrol/alignments/<latest>/part-00001.parquet
Cible  : index Elasticsearch "gaia-alignments"
"""
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
import os
import numpy as np

import pandas as pd
from elasticsearch import Elasticsearch, helpers

# =============================================================================
# CONFIGURATION
# =============================================================================

ES_HOST    = os.getenv("ES_HOST", "http://elasticsearch:9200")
INDEX_NAME = "gaia-alignments"

# ── Résolution dynamique du parquet le plus récent ────────────────────────────
def get_latest_parquet(base_path: Path) -> Path:
    """Prend automatiquement le dossier de date le plus récent."""
    dated_files = sorted(base_path.glob("*/part-00001.parquet"), reverse=True)
    if not dated_files:
        print(f"Aucun parquet trouvé dans {base_path}")
        sys.exit(2)
    latest = dated_files[0]
    print(f"Parquet sélectionné : {latest}")
    return latest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARQUET_PATH = get_latest_parquet(PROJECT_ROOT / "datalake" / "enriched" / "biocontrol" / "alignments")

# Colonnes internes DBT/pipeline qu'on ne veut pas indexer dans ES
COLS_TO_DROP = [
    "source_file_dephy", "source_file_agrovoc",
    "source_sheet", "source_dir",
    "data_layer_dephy", "data_layer_agrovoc",
]

# Mapping ES : on définit le type des champs importants
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "method_name":          {"type": "keyword"},
            "family":               {"type": "keyword"},
            "sub_family":           {"type": "keyword"},
            "crops":                {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "satisfaction_level":   {"type": "keyword"},
            "sector":               {"type": "keyword"},
            "agrovoc_label":        {"type": "keyword"},
            "agrovoc_concept_id":   {"type": "keyword"},
            "agrovoc_uri":          {"type": "keyword"},
            "is_aligned":           {"type": "boolean"},
            "combined_at":          {"type": "date"},
            "indexed_at":           {"type": "date"},
        }
    }
}

# =============================================================================
# FONCTION 1 : Charger le Parquet
# =============================================================================

def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"Fichier introuvable : {path}")
        print("Avez-vous bien execute : dbt run + export DuckDB vers Parquet ?")
        sys.exit(2)

    df = pd.read_parquet(path)
    cols_present = [c for c in COLS_TO_DROP if c in df.columns]
    df = df.drop(columns=cols_present)

    print(f"Parquet charge : {len(df)} lignes, {len(df.columns)} colonnes")
    return df

# =============================================================================
# FONCTION 2 : Connexion Elasticsearch
# =============================================================================

def connect_es(host: str) -> Elasticsearch:
    client = Elasticsearch([host])
    if not client.ping():
        print(f"Impossible de contacter Elasticsearch sur {host}")
        print("Verifiez que le container ES tourne : docker-compose up -d")
        sys.exit(3)

    version = client.info()["version"]["number"]
    print(f"Connecte a Elasticsearch {version} sur {host}")
    return client

# =============================================================================
# FONCTION 3 : Créer l'index avec le mapping
# =============================================================================

def create_index(es: Elasticsearch, index_name: str) -> None:
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Ancien index {index_name} supprime")

    es.indices.create(index=index_name, body=INDEX_MAPPING)
    print(f"Index {index_name} cree avec le mapping")

# =============================================================================
# FONCTION 4 : Préparer les documents pour ES
# =============================================================================

def prepare_docs(df: pd.DataFrame, index_name: str):
    """
    Generateur : convertit chaque ligne en document ES.
    - Remplace les NaN pandas par None (JSON-compatible)
    - Convertit les types numpy en types Python natifs
    - Corrige le format de date pour ES (ISO 8601)
    - Ajoute indexed_at pour la tracabilite
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    df = df.reset_index(drop=True)
    for idx, row in df.iterrows():
        doc = {}
        for k, v in row.to_dict().items():
            if pd.isna(v):
                doc[k] = None
            elif isinstance(v, (np.integer,)):
                doc[k] = int(v)
            elif isinstance(v, (np.floating,)):
                doc[k] = float(v)
            elif isinstance(v, (np.bool_,)):
                doc[k] = bool(v)
            else:
                doc[k] = v

        # Corriger le format de date pour ES (espace → T pour ISO 8601)
        if doc.get("combined_at") and isinstance(doc["combined_at"], str):
            doc["combined_at"] = doc["combined_at"].replace(" ", "T")

        doc["indexed_at"] = now_utc

        yield {
            "_index":  index_name,
            "_id":     idx,
            "_source": doc,
        }

# =============================================================================
# FONCTION 5 : Indexation bulk
# =============================================================================

def bulk_index(es: Elasticsearch, doc_generator) -> Tuple[int, int]:
    success, failed = helpers.bulk(
        es,
        doc_generator,
        stats_only=True,
        raise_on_error=False,
    )
    print(f"Indexation bulk : {success} succes, {failed} echecs")
    return success, failed

# =============================================================================
# FONCTION 6 : Vérification
# =============================================================================

def verify_index(es: Elasticsearch, index_name: str) -> None:
    count = es.count(index=index_name)["count"]
    print(f"Verification : {count} documents dans l'index {index_name}")

    result = es.search(
        index=index_name,
        body={"size": 3, "query": {"match_all": {}}}
    )

    print("\nExemples de documents indexes :")
    for hit in result["hits"]["hits"]:
        src = hit["_source"]
        print(
            f"  methode={src.get('method_name')} | "
            f"famille={src.get('family')} | "
            f"agrovoc={src.get('agrovoc_label')} | "
            f"aligne={src.get('is_aligned')}"
        )

# =============================================================================
# MAIN
# =============================================================================

def main():
    df = load_parquet(PARQUET_PATH)
    es = connect_es(ES_HOST)
    create_index(es, INDEX_NAME)
    docs = prepare_docs(df, INDEX_NAME)
    bulk_index(es, docs)
    es.indices.refresh(index=INDEX_NAME)
    verify_index(es, INDEX_NAME)

if __name__ == "__main__":
    main()