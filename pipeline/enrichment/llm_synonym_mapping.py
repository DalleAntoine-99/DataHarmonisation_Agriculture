import json
import os
import sys
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://host.docker.internal:1234/v1/chat/completions")
MODEL         = "google/gemma-3-12b"
BASE_DIR      = Path(__file__).resolve().parent.parent.parent   # → /opt/airflow/project
RUN_DATE      = datetime.now(timezone.utc).strftime("%Y%m%d")

# ── Résolution dynamique ───────────────────────────────────────────────────────
def get_latest_parquet(base_path: Path) -> Path:
    files = sorted(base_path.glob("*/part-00001.parquet"), reverse=True)
    if not files:
        print(f"Aucun parquet trouvé dans {base_path}")
        sys.exit(2)
    latest = files[0]
    print(f"Parquet sélectionné : {latest}")
    return latest

# ── Chemins datalake (respecte la hiérarchie raw/formatted/enriched) ───────────
AGROVOC_PARQUET    = get_latest_parquet(BASE_DIR / "datalake/formatted/agrovoc/concepts")
ALIGNMENTS_PARQUET = get_latest_parquet(BASE_DIR / "datalake/enriched/biocontrol/alignments")
RAW_OUTPUT_JSON    = BASE_DIR / f"datalake/raw/llm/synonyms/{RUN_DATE}/llm_synonyms_raw.json"
FORMATTED_PARQUET  = BASE_DIR / f"datalake/formatted/llm/synonyms/{RUN_DATE}/llm_synonyms.parquet"
SEED_CSV           = BASE_DIR / "Data_Harmo_dbt/seeds/llm_synonyms.csv"


# ── 1. Charger les données ──────────────────────────────────────────────────────
def load_data():
    print("Chargement des données...")
    alignments_df = pd.read_parquet(ALIGNMENTS_PARQUET)
    agrovoc_df    = pd.read_parquet(AGROVOC_PARQUET)

    all_unaligned = set(
        alignments_df[alignments_df["is_aligned"] == False]["method_name"]
        .dropna().unique().tolist()
    )
    agrovoc_terms = sorted(agrovoc_df["dephy_term"].dropna().unique().tolist())
    agrovoc_lookup = agrovoc_df[
        ["dephy_term", "agrovoc_concept_id", "agrovoc_uri", "agrovoc_label"]
    ].drop_duplicates()

    return all_unaligned, agrovoc_terms, agrovoc_lookup


# ── 2. Logique incrémentale ─────────────────────────────────────────────────────
def get_methods_to_process(all_unaligned: set) -> list:
    if SEED_CSV.exists():
        existing = pd.read_csv(SEED_CSV)
        already_mapped = set(existing["dephy_term"].tolist())
        print(f"Seed existant       : {len(already_mapped)} mappings déjà connus")
    else:
        already_mapped = set()
        print("Pas de seed existant — premier run")

    to_process = sorted(all_unaligned - already_mapped)

    print(f"Total non alignées  : {len(all_unaligned)}")
    print(f"Déjà traitées       : {len(already_mapped)}")
    print(f"Restant à envoyer   : {len(to_process)}")
    return to_process


# ── 3. Appel LLM ───────────────────────────────────────────────────────────────
def call_llm(methods_to_process: list, agrovoc_terms: list) -> dict:
    prompt = f"""Tu es un expert en agronomie et en protection des cultures.

Voici une liste de méthodes de biocontrôle issues de fiches terrain françaises (réseau DEPHY)
qui n'ont pas pu être alignées automatiquement avec un thésaurus international (AGROVOC) :

MÉTHODES DEPHY NON ALIGNÉES :
{json.dumps(methods_to_process, ensure_ascii=False, indent=2)}

Voici les termes AGROVOC disponibles (ce sont les SEULES valeurs autorisées) :
{json.dumps(agrovoc_terms, ensure_ascii=False, indent=2)}

Ta tâche : pour chaque méthode DEPHY, trouve le terme AGROVOC le plus proche sémantiquement.

Règles STRICTES :
- Tu ne peux utiliser QUE les termes de la liste AGROVOC fournie ci-dessus
- Si aucun terme ne correspond, mets null
- Ne crée pas de nouveaux termes
- Réponds UNIQUEMENT avec un JSON valide, sans texte avant ni après
- Format exact attendu :

{{
  "synonyms_mapping": {{
    "méthode DEPHY": "terme AGROVOC ou null"
  }}
}}
"""
    print("\nEnvoi du prompt à Gemma 3 12B...")
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Tu es un expert en agronomie. Tu réponds uniquement en JSON valide, sans markdown, sans explication."
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 4096
    }

    response = requests.post(LM_STUDIO_URL, json=payload)
    response.raise_for_status()

    raw_content = response.json()["choices"][0]["message"]["content"]
    print(f"Réponse reçue ({len(raw_content)} caractères)")

    # Nettoyer si markdown
    clean = raw_content.strip()
    if clean.startswith("```"):
        lines = clean.split("\n")
        clean = "\n".join(lines[1:-1])

    return json.loads(clean)


# ── 4. Sauvegarder en raw (JSON brut immuable) ─────────────────────────────────
def save_raw(result: dict):
    RAW_OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    # Charger historique si existant et merger
    history = {"synonyms_mapping": {}}
    existing_raws = sorted(
        (BASE_DIR / "datalake/raw/llm/synonyms").glob("*/llm_synonyms_raw.json")
    ) if (BASE_DIR / "datalake/raw/llm/synonyms").exists() else []

    for past_file in existing_raws:
        with open(past_file, "r") as f:
            past = json.load(f)
            history["synonyms_mapping"].update(past.get("synonyms_mapping", {}))

    history["synonyms_mapping"].update(result["synonyms_mapping"])
    history["run_date"] = RUN_DATE

    with open(RAW_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print(f"Raw sauvegardé      : {RAW_OUTPUT_JSON}")
    return history


# ── 5. Formater et sauvegarder en parquet ──────────────────────────────────────
def save_formatted(new_mappings: dict, agrovoc_lookup: pd.DataFrame):
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []

    for dephy_method, agrovoc_term in new_mappings.items():
        if agrovoc_term is None:
            continue
        match = agrovoc_lookup[agrovoc_lookup["dephy_term"] == agrovoc_term]
        if not match.empty:
            rows.append({
                "dephy_term":            dephy_method,
                "dephy_term_normalized": dephy_method.lower().strip(),
                "matched_query":         agrovoc_term,
                "agrovoc_concept_id":    match.iloc[0]["agrovoc_concept_id"],
                "agrovoc_uri":           match.iloc[0]["agrovoc_uri"],
                "agrovoc_label":         match.iloc[0]["agrovoc_label"],
                "source":                "llm_gemma3",
                "formatted_at":          run_ts,
                "data_layer":            "formatted"
            })

    new_df = pd.DataFrame(rows)
    FORMATTED_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    new_df.to_parquet(FORMATTED_PARQUET, index=False)
    print(f"Formatted sauvegardé : {FORMATTED_PARQUET} ({len(new_df)} lignes)")
    return new_df


# ── 6. Mettre à jour le seed DBT (append + dedup) ─────────────────────────────
def update_seed(new_df: pd.DataFrame):
    SEED_CSV.parent.mkdir(parents=True, exist_ok=True)

    if SEED_CSV.exists():
        existing_df = pd.read_csv(SEED_CSV)
        final_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(
            subset=["dephy_term"]
        )
    else:
        final_df = new_df

    final_df.to_csv(SEED_CSV, index=False)
    print(f"Seed mis à jour      : {SEED_CSV} ({len(final_df)} lignes total)")
    return final_df


# ── 7. Statistiques ────────────────────────────────────────────────────────────
def print_stats(new_mappings: dict, final_seed: pd.DataFrame):
    mapped     = {k: v for k, v in new_mappings.items() if v is not None}
    not_mapped = {k: v for k, v in new_mappings.items() if v is None}

    print("\n" + "="*50)
    print("RÉSULTATS CE RUN")
    print("="*50)
    print(f"Nouveaux mappings trouvés : {len(mapped)}")
    print(f"Sans correspondance       : {len(not_mapped)}")
    print(f"Total seed CSV            : {len(final_seed)}")

    print("\nExemples de mappings :")
    for k, v in list(mapped.items())[:10]:
        print(f"  '{k}' → '{v}'")

    if not_mapped:
        print(f"\nToujours sans match ({len(not_mapped)}) :")
        for k in list(not_mapped.keys())[:10]:
            print(f"  - {k}")
    print("="*50)


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Charger
    all_unaligned, agrovoc_terms, agrovoc_lookup = load_data()

    # 2. Déterminer ce qu'il reste à traiter
    methods_to_process = get_methods_to_process(all_unaligned)
    if not methods_to_process:
        print("\nTout est déjà mappé — rien à faire.")
        exit(0)

    # 3. Appeler le LLM
    result = call_llm(methods_to_process, agrovoc_terms)
    new_mappings = result["synonyms_mapping"]

    # 4. Sauvegarder en raw (immuable)
    save_raw(result)

    # 5. Formater et sauvegarder en parquet
    new_df = save_formatted(new_mappings, agrovoc_lookup)

    # 6. Mettre à jour le seed DBT
    final_seed = update_seed(new_df)

    # 7. Afficher les stats
    print_stats(new_mappings, final_seed)