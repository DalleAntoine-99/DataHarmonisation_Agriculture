import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import os

def clean_doc(row: dict) -> dict:
    """Nettoie les valeurs NaN et ajoute un champ d'indexation."""
    return {
        k.strip(): (None if pd.isna(v) else v)
        for k, v in row.items()
    } | {"indexed_at": datetime.utcnow().isoformat()}

def main():
    parser = argparse.ArgumentParser(description="Indexe DEPHY BDD_méthodes dans Elasticsearch")
    parser.add_argument("--src", required=True, help="Chemin du fichier xlsx")
    parser.add_argument("--sheet", default="BDD_méthodes", help="Nom de l'onglet (par défaut: BDD_méthodes)")
    parser.add_argument("--index", default="dephy-methodes", help="Nom de l'index ES")
    parser.add_argument("--es", default=os.getenv("ES_HOST", "http://elasticsearch:9200"), help="URL Elasticsearch")

    args = parser.parse_args()

    src = Path(args.src)
    if not src.exists():
        print("❌ Fichier non trouvé:", src, file=sys.stderr)
        sys.exit(2)

    print(f"📂 Lecture {src.name} (onglet '{args.sheet}') ...")
    df = pd.read_excel(src, sheet_name=args.sheet)
    print(f"   → {len(df)} lignes, {len(df.columns)} colonnes")

    # Nettoyage des colonnes (supprime espaces)
    df.columns = [str(c).strip() for c in df.columns]

    client = Elasticsearch(hosts=[args.es])
    if not client.ping():
        print("Impossible de contacter Elasticsearch à", args.es, file=sys.stderr)
        sys.exit(2)

    docs = [clean_doc(row) for row in df.to_dict(orient="records")]

    success, errors = helpers.bulk(client, docs, index=args.index, raise_on_error=False)
    print(f"{success} documents indexés dans '{args.index}'")
    if errors:
        print(f"{len(errors)} erreurs:")
        for e in errors[:3]:
            print(" ", e)

if __name__ == "__main__":
    main()
    