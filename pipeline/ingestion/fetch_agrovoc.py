"""
Fetch AGROVOC concepts via API FAO pour chaque terme DEPHY unique.
Stocke les résultats bruts en JSON + indexe dans Elasticsearch.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict


import requests
from elasticsearch import Elasticsearch, helpers

# ── Config ────────────────────────────────────────────────────────────────────
ES_HOST  = "http://elasticsearch:9200"
ES_INDEX = "agrovoc-concepts"
PROJECT_ROOT = Path("/opt/airflow/project")
RAW_DIR = PROJECT_ROOT / "datalake/raw/agrovoc" / datetime.now(timezone.utc).strftime("%Y%m%d")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Termes DEPHY avec variantes pour améliorer le recall ─────────────────────
# Format : { "terme_principal": ["variante1", "variante2", ...] }
DEPHY_TERMS_WITH_VARIANTS = {
    # Familles
    "biological control":       ["biocontrol", "biological pest control"],
    "macro-organisms":          ["macroorganisms", "beneficial insects", "natural enemies"],
    "micro-organisms":          ["microorganisms", "microbial control"],
    "semiochemicals":           ["pheromones", "kairomones", "allelochemicals"],
    "service plants":           ["cover crops", "companion plants", "intercropping"],
    "natural substances":       ["biopesticides", "plant extracts", "botanical pesticides"],
    # Sous-familles
    "predatory mites":          ["Phytoseiidae", "mite predators"],
    "organic amendment":        ["organic matter", "compost", "manure"],
    "barriers":                 ["physical barriers", "insect nets"],
    "parasitoid insects":       ["parasitoids", "hymenoptera parasitoids"],
    "predatory insects":        ["insect predators", "beneficial insects"],
    "entomopathogenic nematodes": ["Steinernema", "Heterorhabditis"],
    "pheromones":               ["sex pheromones", "aggregation pheromones"],
    "trapping":                 ["insect traps", "sticky traps", "pheromone traps"],
    "biofumigation":            ["biofumigation plants", "glucosinolates"],
    "nematicidal plants":       ["nematode resistant plants", "Tagetes"],
    "trap plants":              ["trap cropping", "banker plants"],
    "repellent plants":         ["plant repellents", "aromatic plants"],
    "plant defense stimulator": ["plant resistance inducers", "elicitors"],
    # Noms méthodes scientifiques
    "Bacillus subtilis":        [],
    "Bacillus thuringiensis":   ["Bt"],
    "Trichoderma harzianum":    ["Trichoderma"],
    "Trichoderma asperellum":   ["Trichoderma"],
    "Pythium oligandrum":       [],
    "Coniothyrium minitans":    ["Contans"],
    "Lecanicillium muscarium":  ["Verticillium lecanii"],
    "Neoseiulus californicus":  [],
    "Neoseiulus cucumeris":     [],
    "Phytoseiulus persimilis":  [],
    "Macrolophus pygmaeus":     ["Macrolophus caliginosus"],
    "Orius laevigatus":         ["Orius"],
    "Chrysoperla carnea":       ["lacewings"],
    "Aphidius colemani":        [],
    "Aphidius ervi":            [],
    "Encarsia formosa":         [],
    "Trichogramma":             [],
    "Steinernema carpocapsae":  ["entomopathogenic nematodes"],
    "kaolin":                   ["kaolin clay"],
    "spinosad":                 [],
    "chitosan":                 [],
    "laminarin":                ["laminaria", "seaweed extract"],
    "potassium bicarbonate":    ["potassium hydrogen carbonate"],
    "phosphonate":              ["phosphorous acid"],
    "propolis":                 [],
    "confusion technique":      ["mating disruption", "sex pheromone disruption"],
    "pheromone trap":           ["pheromone traps", "insect monitoring"],
    "cover crop":               ["green manure", "catch crop"],
    "companion planting":       ["intercropping", "mixed cropping"],
    "mustard green manure":     ["Brassica", "green manure"],
}

# ── Connexion ES ──────────────────────────────────────────────────────────────
def get_es_client() -> Elasticsearch:
    client = Elasticsearch(hosts=[ES_HOST])
    if not client.ping():
        print("❌ Impossible de contacter Elasticsearch à", ES_HOST, file=sys.stderr)
        sys.exit(2)
    print("✅ Connecté à Elasticsearch:", ES_HOST)
    return client

# ── Fetch AGROVOC ─────────────────────────────────────────────────────────────
def fetch_agrovoc(query: str, lang: str = "en", rows: int = 5) -> List[Dict]:
    url = "https://agrovoc.fao.org/browse/rest/v1/agrovoc/search"
    try:
        resp = requests.get(
            url,
            params={"query": query, "lang": lang, "rows": rows},
            timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except requests.RequestException as e:
        print(f"   ⚠️  Erreur pour '{query}': {e}", file=sys.stderr)
        return []

# ── Préparer les documents ES ─────────────────────────────────────────────────
def build_docs(dephy_term: str, matched_query: str, results: List[Dict]) -> List[Dict]:
    return [{
        "dephy_term":      dephy_term,
        "matched_query":   matched_query,   # ← variante qui a matché
        "agrovoc_uri":     r.get("uri", ""),
        "agrovoc_label":   r.get("prefLabel", ""),
        "agrovoc_id":      r.get("localname", ""),
        "fetched_at":      datetime.now(timezone.utc).isoformat(),
    } for r in results]

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    client   = get_es_client()
    all_docs = []
    all_raw  = {}
    found    = 0
    not_found = []

    total = len(DEPHY_TERMS_WITH_VARIANTS)
    print(f"\nInterrogation AGROVOC pour {total} termes DEPHY...\n")

    for i, (term, variants) in enumerate(DEPHY_TERMS_WITH_VARIANTS.items()):
        # essayer le terme principal d'abord
        results = fetch_agrovoc(term)
        matched_query = term

        # si 0 résultats → essayer les variantes
        if not results:
            for variant in variants:
                results = fetch_agrovoc(variant)
                if results:
                    matched_query = variant
                    break

        status = f"({matched_query})" if results else "❌ aucun match"
        print(f"   [{i+1}/{total}] '{term}' → {len(results)} résultats {status}")

        if results:
            found += 1
            all_raw[term] = {"matched_query": matched_query, "results": results}
            all_docs.extend(build_docs(term, matched_query, results))
        else:
            not_found.append(term)

        # pause toutes les 10 requêtes
        if (i + 1) % 10 == 0:
            print("   ⏳ Pause 2s...")
            time.sleep(2)

    # ── Résumé ────────────────────────────────────────────────────────────────
    print(f"\n📊 Résumé:")
    print(f"   {found}/{total} termes trouvés dans AGROVOC")
    print(f"   {len(not_found)} termes sans correspondance:")
    for t in not_found:
        print(f"      - {t}")

    # ── Sauvegarder le raw JSON ───────────────────────────────────────────────
    raw_path = RAW_DIR / "agrovoc_dephy_terms.json"
    raw_path.write_text(json.dumps(all_raw, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n💾 Raw JSON sauvegardé: {raw_path}")

    # ── Indexer dans ES ───────────────────────────────────────────────────────
    if all_docs:
        success, errors = helpers.bulk(client, all_docs, index=ES_INDEX, raise_on_error=False)
        print(f"{success} documents indexés dans '{ES_INDEX}'")
        if errors:
            print(f"{len(errors)} erreurs:")
            for e in errors[:3]:
                print(" ", e)
    else:
        print("Aucun document à indexer.")

if __name__ == "__main__":
    main()