"""
Formatting layer — DEPHY
Lit le xlsx brut (BDD_méthodes) et produit un fichier Parquet normalisé
dans datalake/formatted/dephy/variable_cards.parquet
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Chemins ───────────────────────────────────────────────────────────────────
RAW_PATH      = Path("datalake/raw/dephy/variableCards/20260222/fichier-methodes-controle-biologique-expe1.xlsx")
FORMATTED_DIR = Path("datalake/formatted/dephy/variableCards/20260222")
OUTPUT_PATH   = FORMATTED_DIR / "part-00001.parquet"
# ── Mapping colonnes xlsx → noms normalisés snake_case ───────────────────────
COLUMN_MAPPING = {
    "Nom méthode":                                          "method_name",
    "Famille méthode de contrôle biologique":               "family",
    "Sous-famille méthode de contrôle biologique":          "sub_family",
    "Type de traitement":                                   "treatment_type",
    "Partie traitée":                                       "treated_part",
    "Mode d'action":                                        "mode_of_action",
    "Groupe ciblé":                                         "target_group",
    "Cible principale":                                     "main_target",
    "Autres caractéristiques (précision, nom commercial, caractéristiques d'emploi…)": "other_characteristics",
    "Stade d'application":                                  "application_stage",
    "RDD ou OAD":                                           "rdd_or_oad",
    "Nombre d'application":                                 "application_count",
    "Dose application":                                     "application_dose",
    "Niveau de satisfaction de l'utilisation de la méthode": "satisfaction_level",
    "Filière":                                              "sector",
    "Cultures":                                             "crops",
    "Pleine terre ou abri":                                 "outdoor_or_indoor",
    "Période expérimentation":                              "experiment_period",
    "Nom du projet":                                        "project_name",
    "Site expérimental":                                    "experimental_site",
    "Code postal site":                                     "site_postal_code",
    "Nom du système":                                       "system_name",
    "Lien fiche SYSTEME":                                   "system_link",
}

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Nettoyer les noms de colonnes (supprimer espaces)
    df.columns = [c.strip() for c in df.columns]

    # 2) Renommer en snake_case
    df = df.rename(columns=COLUMN_MAPPING)

    # 3) Nettoyer les valeurs texte (strip)
    text_cols = df.select_dtypes(include="object").columns
    for col in text_cols:
        df[col] = df[col].str.strip() if hasattr(df[col], "str") else df[col]

    # 4) Normaliser les valeurs catégorielles (minuscules)
    for col in ["family", "sub_family", "treatment_type", "mode_of_action",
                "target_group", "sector", "satisfaction_level"]:
        if col in df.columns:
            df[col] = df[col].str.lower().str.strip()

    # 5) Ajouter les métadonnées de traçabilité (UTC)
    now_utc = datetime.now(timezone.utc).isoformat()
    df["source_file"]    = RAW_PATH.name
    df["source_sheet"]   = "BDD_méthodes"
    df["formatted_at"]   = now_utc
    df["data_layer"]     = "formatted"

    # 6) Supprimer les lignes sans method_name (lignes vides)
    df = df.dropna(subset=["method_name"])

    # 7) Reset index propre
    df = df.reset_index(drop=True)

    return df

def main():
    if not RAW_PATH.exists():
        print(f"Fichier source introuvable: {RAW_PATH}", file=sys.stderr)
        sys.exit(2)

    print(f"Lecture de {RAW_PATH.name} ...")
    df = pd.read_excel(RAW_PATH, sheet_name="BDD_méthodes")
    print(f"   → {len(df)} lignes brutes, {len(df.columns)} colonnes")

    print("Normalisation en cours ...")
    df_clean = normalize(df)
    print(f"   → {len(df_clean)} lignes après nettoyage")
    print(f"   → Colonnes: {df_clean.columns.tolist()}")

    # Créer le dossier formatted si besoin
    FORMATTED_DIR.mkdir(parents=True, exist_ok=True)

    # Sauvegarder en Parquet
    df_clean.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")
    print(f"\n✅ Parquet sauvegardé: {OUTPUT_PATH}")
    print(f"   → Taille: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")

    # Aperçu rapide
    print(f"\nAperçu des 3 premières lignes:")
    print(df_clean[["method_name", "family", "sub_family", "crops", "satisfaction_level"]].head(3).to_string())

if __name__ == "__main__":
    main()