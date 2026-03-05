import json
import os
import sys
import argparse
import hashlib
import requests
import pdfplumber
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

# ── Config ─────────────────────────────────────────────────────────────────────
LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://host.docker.internal:1234/v1/chat/completions")
MODEL         = "google/gemma-3-12b"
BASE_DIR      = Path(__file__).resolve().parent.parent.parent
RUN_DATE      = datetime.now(timezone.utc).strftime("%Y%m%d")

MAX_TEXT_CHARS = 6000   # ~1500 tokens — limite raisonnable pour Gemma 12B
TEMPERATURE    = 0.05

# ── Chemins datalake ───────────────────────────────────────────────────────────
def get_latest_parquet(base_path: Path) -> Path:
    files = sorted(base_path.glob("*/part-00001.parquet"), reverse=True)
    if not files:
        print(f"Aucun parquet trouvé dans {base_path}")
        sys.exit(2)
    return files[0]

DEPHY_PARQUET      = get_latest_parquet(BASE_DIR / "datalake/formatted/dephy/variableCards")
RAW_PDF_CACHE_DIR  = BASE_DIR / "datalake/raw/pdfs"
RAW_OUTPUT_JSON    = BASE_DIR / f"datalake/raw/llm/pdf_extracts/{RUN_DATE}/llm_pdf_raw.json"
FORMATTED_PARQUET  = BASE_DIR / f"datalake/formatted/llm/pdf_extracts/{RUN_DATE}/llm_pdf_extracts.parquet"
SEED_CSV           = BASE_DIR / "Data_Harmo_dbt/seeds/llm_pdf_extracts.csv"


# ── 1. Charger le parquet DEPHY ────────────────────────────────────────────────
def load_dephy() -> pd.DataFrame:
    print("Chargement des données DEPHY...")
    df = pd.read_parquet(DEPHY_PARQUET)
    df = df[df["system_link"].notna() & (df["system_link"].str.strip() != "")]
    print(f"  → {len(df)} lignes avec system_link")
    print(f"  → {df['system_link'].nunique()} PDFs uniques")
    return df


# ── 2. Logique incrémentale ────────────────────────────────────────────────────
def get_rows_to_process(df: pd.DataFrame) -> pd.DataFrame:
    if SEED_CSV.exists():
        existing = pd.read_csv(SEED_CSV)
        already_done = set(
            zip(existing["method_name"], existing["system_link"])
        )
        mask = df.apply(
            lambda r: (r["method_name"], r["system_link"]) not in already_done,
            axis=1
        )
        to_process = df[mask].copy()
        print(f"Seed existant    : {len(existing)} extractions déjà connues")
    else:
        to_process = df.copy()
        print("Pas de seed existant — premier run")

    print(f"Total lignes     : {len(df)}")
    print(f"Restant à traiter: {len(to_process)}")
    return to_process


# ── 3. Download + OCR avec cache par URL ──────────────────────────────────────
def url_to_hash(url: str) -> str:
    return hashlib.md5(url.strip().encode()).hexdigest()[:12]


def get_pdf_text(url: str) -> Optional[str]:
    """Télécharge le PDF et extrait le texte. Cache sur disque par hash d'URL."""
    pdf_hash  = url_to_hash(url)
    cache_dir = RAW_PDF_CACHE_DIR / pdf_hash
    cache_txt = cache_dir / "text.txt"
    cache_url = cache_dir / "source_url.txt"

    # ── Cache hit ──────────────────────────────────────────────────────────────
    if cache_txt.exists():
        print(f"  [CACHE HIT]  {pdf_hash} ← {url[-50:]}")
        return cache_txt.read_text(encoding="utf-8")

    # ── Download ───────────────────────────────────────────────────────────────
    print(f"  [DOWNLOAD]   {url[-60:]}")
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"  [ERREUR DOWNLOAD] {e}")
        return None

    # ── OCR pdfplumber ─────────────────────────────────────────────────────────
    try:
        text_parts = []
        with pdfplumber.open(BytesIO(resp.content)) as pdf:
            print(f"  [OCR]        {len(pdf.pages)} pages")
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        full_text = "\n".join(text_parts).strip()
    except Exception as e:
        print(f"  [ERREUR OCR] {e}")
        return None

    if not full_text:
        print(f"  [VIDE]       aucun texte extrait")
        return None

    # ── Sauvegarde cache ───────────────────────────────────────────────────────
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_txt.write_text(full_text, encoding="utf-8")
    cache_url.write_text(url, encoding="utf-8")
    print(f"  [CACHE SAVE] {len(full_text)} caractères → {cache_txt}")

    return full_text


# ── 4. Appel LLM ───────────────────────────────────────────────────────────────
def call_llm(row: pd.Series, pdf_text: str) -> Optional[dict]:
    # Tronquer le texte si trop long
    truncated = pdf_text[:MAX_TEXT_CHARS]
    if len(pdf_text) > MAX_TEXT_CHARS:
        truncated += "\n[... texte tronqué ...]"

    prompt = f"""Tu es un expert en agronomie et biocontrôle.

Voici une fiche système DEPHY extraite d'un PDF. Cette fiche décrit un système de culture complet.
Tu dois extraire les informations relatives à la méthode de biocontrôle suivante UNIQUEMENT :

MÉTHODE CIBLÉE :
- Nom          : {row.get('method_name', 'N/A')}
- Famille      : {row.get('family', 'N/A')}
- Sous-famille : {row.get('sub_family', 'N/A')}
- Culture      : {row.get('crops', 'N/A')}
- Cible        : {row.get('main_target', 'N/A')}

TEXTE DU PDF :
{truncated}

Ta tâche : extraire uniquement les informations PRÉSENTES dans le texte ci-dessus concernant cette méthode.

Règles STRICTES :
- N'invente RIEN — si une information est absente, mets null
- Réponds UNIQUEMENT avec un JSON valide, sans texte avant ni après
- Format exact :

{{
  "efficacy_observed": "description de l'efficacité observée ou null",
  "application_conditions": "conditions d'application mentionnées ou null",
  "results_summary": "résumé des résultats ou null",
  "limitations": "limites ou contraintes mentionnées ou null",
  "confidence": 0.0
}}

Le champ "confidence" est entre 0.0 et 1.0 selon la pertinence des infos trouvées.
"""

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Tu es un expert en agronomie. Tu réponds uniquement en JSON valide, sans markdown, sans explication."
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": TEMPERATURE,
        "max_tokens": 512
    }

    try:
        response = requests.post(LM_STUDIO_URL, json=payload, timeout=120)
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"].strip()

        # Nettoyer si markdown
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1])

        return json.loads(raw)

    except Exception as e:
        print(f"  [ERREUR LLM] {e}")
        return None


# ── 5. Sauvegarder raw ─────────────────────────────────────────────────────────
def save_raw(all_results: list):
    RAW_OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(RAW_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"run_date": RUN_DATE, "extractions": all_results}, f, ensure_ascii=False, indent=2)
    print(f"Raw sauvegardé   : {RAW_OUTPUT_JSON} ({len(all_results)} entrées)")


# ── 6. Formater et sauvegarder parquet ────────────────────────────────────────
def save_formatted(all_results: list) -> pd.DataFrame:
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for r in all_results:
        if r.get("llm_result") and r["llm_result"].get("confidence", 0) > 0.2:
            rows.append({
                "method_name":            r["method_name"],
                "system_link":            r["system_link"],
                "pdf_hash":               r["pdf_hash"],
                "efficacy_observed":      r["llm_result"].get("efficacy_observed"),
                "application_conditions": r["llm_result"].get("application_conditions"),
                "results_summary":        r["llm_result"].get("results_summary"),
                "limitations":            r["llm_result"].get("limitations"),
                "confidence":             r["llm_result"].get("confidence", 0.0),
                "source":                 "llm_pdf_gemma3",
                "formatted_at":           run_ts,
                "data_layer":             "formatted"
            })

    df = pd.DataFrame(rows)
    FORMATTED_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(FORMATTED_PARQUET, index=False)
    print(f"Formatted sauvegardé : {FORMATTED_PARQUET} ({len(df)} lignes)")
    return df


# ── 7. Mettre à jour le seed CSV ───────────────────────────────────────────────
def update_seed(new_df: pd.DataFrame) -> pd.DataFrame:
    SEED_CSV.parent.mkdir(parents=True, exist_ok=True)
    if SEED_CSV.exists():
        existing_df = pd.read_csv(SEED_CSV)
        final_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(
            subset=["method_name", "system_link"]
        )
    else:
        final_df = new_df

    final_df.to_csv(SEED_CSV, index=False)
    print(f"Seed mis à jour  : {SEED_CSV} ({len(final_df)} lignes total)")
    return final_df


# ── 8. Statistiques ────────────────────────────────────────────────────────────
def print_stats(all_results: list, final_seed: pd.DataFrame):
    success   = [r for r in all_results if r.get("llm_result")]
    failures  = [r for r in all_results if not r.get("llm_result")]
    high_conf = [r for r in success if r["llm_result"].get("confidence", 0) > 0.6]

    print("\n" + "="*50)
    print("RÉSULTATS CE RUN")
    print("="*50)
    print(f"Extractions réussies      : {len(success)}")
    print(f"Haute confiance (>0.6)    : {len(high_conf)}")
    print(f"Échecs (download/LLM)     : {len(failures)}")
    print(f"Total seed CSV            : {len(final_seed)}")
    print("="*50)


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Nombre de lignes à traiter (None = tout)")
    args = parser.parse_args()

    # 1. Charger DEPHY
    dephy_df = load_dephy()

    # 2. Filtrer les lignes déjà traitées
    to_process = get_rows_to_process(dephy_df)
    if to_process.empty:
        print("Tout est déjà traité — rien à faire.")
        sys.exit(0)

    # ── LIMIT ─────────────────────────────────────────────────────────────────
    if args.limit:
        to_process = to_process.head(args.limit)
        print(f"Mode test : limité à {args.limit} ligne(s)")

    # 3. Boucle principale
    all_results = []
    pdf_cache   = {}   # url → texte (cache mémoire session)

    for idx, row in to_process.iterrows():
        url = row["system_link"].strip()
        print(f"\n[{idx+1}/{len(to_process)}] {row['method_name']}")

        # OCR (cache disque + mémoire)
        if url not in pdf_cache:
            pdf_cache[url] = get_pdf_text(url)
        pdf_text = pdf_cache[url]

        if pdf_text is None:
            all_results.append({
                "method_name": row["method_name"],
                "system_link": url,
                "pdf_hash":    url_to_hash(url),
                "llm_result":  None,
                "error":       "download_or_ocr_failed"
            })
            continue

        llm_result = call_llm(row, pdf_text)
        all_results.append({
            "method_name": row["method_name"],
            "system_link": url,
            "pdf_hash":    url_to_hash(url),
            "llm_result":  llm_result
        })

        # ← NOUVEAU : sauvegarde intermédiaire toutes les 10 lignes
        if len(all_results) % 10 == 0:
            save_raw(all_results)
            print(f"  [CHECKPOINT] {len(all_results)} résultats sauvegardés")

    # 4. Sauvegarder raw
    save_raw(all_results)

    # 5. Formater
    new_df = save_formatted(all_results)

    # 6. Mettre à jour seed
    final_seed = update_seed(new_df)

    # 7. Stats
    print_stats(all_results, final_seed)