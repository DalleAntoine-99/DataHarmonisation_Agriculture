# PHYTOLEX — Harmonisation Sémantique des Données Agricoles

**Cours :** DATA 705 — Data Lake — Février 2026 — Télécom Paris  
**Auteur :** Antoine Dalle

---

## Contexte

L'ACTA dispose de multiples sources de données agricoles (DEPHY, AGROVOC, publications PDF) qui utilisent des vocabulaires différents. PHYTOLEX aligne automatiquement ces vocabulaires pour permettre une exploitation croisée des données.

---

## Stack technique

| Composant | Technologie |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Transformation | DBT + DuckDB |
| Enrichissement IA | LM Studio (Gemma 3 12B) |
| Indexation | Elasticsearch 8.19 |
| Visualisation | Kibana 8.19 |
| Containerisation | Docker Compose |

---

## Structure du projet

```
Data_Harmonisation/
│
├── airflow/dags/
│   ├── gaia_pipeline.py              # DAG principal
│   ├── dag_llm_synonyms.py          # DAG enrichissement LLM
│   └── dag_llm_pdf_extract.py       # DAG extraction PDF
│
├── pipeline/
│   ├── ingestion/
│   │   ├── fetch_agrovoc.py          # API REST AGROVOC FAO
│   │   ├── index_dephy_methodes.py   # Indexation DEPHY
│   │   └── llm_pdf_extract.py        # Extraction PDF via LLM
│   ├── formatting/
│   │   ├── normalize_dephy.py        # XLSX → Parquet
│   │   └── normalize_agrovoc.py      # JSON → Parquet
│   ├── enrichment/
│   │   └── llm_synonym_mapping.py    # Alignement sémantique LLM
│   ├── export/
│   │   └── export_parquet.py         # DBT → Parquet enriched
│   └── indexing/
│       └── index_alignments.py       # Parquet → Elasticsearch
│
├── Data_Harmo_dbt/
│   ├── seeds/                        # llm_synonyms.csv
│   ├── models/staging/               # stg_dephy.sql, stg_agrovoc.sql
│   └── models/marts/                 # mart_alignments.sql
│
├── datalake/
│   ├── raw/                          # Données brutes
│   ├── formatted/                    # Données normalisées (Parquet)
│   └── enriched/                     # Données combinées (Parquet)
│
├── kibana/
│   └── kibana_phytolex_export.ndjson # Dashboard exporté (importable)
│
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

---

## Installation

### Prérequis

- Docker & Docker Compose
- Python 3.10+
- (Optionnel) LM Studio avec Gemma 3 12B pour l'enrichissement LLM

### 1. Lancer les services

```bash
cd Data_Harmonisation
docker compose up -d --build
```

### 2. Vérifier que tout tourne

```bash
docker ps
```

Vous devez voir 3 containers :
- `data_harmonisation-airflow-1` → port 8080
- `data_harmonisation-elasticsearch-1` → port 9200
- `data_harmonisation-kibana-1` → port 5601

---

## Utilisation

### Lancer le pipeline principal

**Option A — Via l'interface Airflow**

1. Ouvrir http://localhost:8080 (login : `airflow` / `airflow`)
2. Trouver le DAG `gaia_pipeline`
3. Cliquer sur le bouton ▶️ Trigger DAG

**Option B — Via le terminal**

```bash
docker exec -it data_harmonisation-airflow-1 \
  airflow dags trigger gaia_pipeline
```

### Lancer l'enrichissement LLM (optionnel)

> Nécessite LM Studio actif sur le port 1234 avec Gemma 3 12B chargé.

```bash
# Synonymes LLM
docker exec -it data_harmonisation-airflow-1 \
  airflow dags trigger gaia_llm_synonyms

# Extraction PDF
docker exec -it data_harmonisation-airflow-1 \
  airflow dags trigger gaia_llm_pdf_extract
```

Ces deux DAGs déclenchent automatiquement `gaia_pipeline` à la fin.

---

## Importer le dashboard Kibana

Le dashboard est pré-exporté dans `kibana/kibana_phytolex_export.ndjson`.

**Option A — Via l'interface Kibana**

1. Ouvrir http://localhost:5601
2. Aller dans **Stack Management** → **Saved Objects**
3. Cliquer **Import**
4. Sélectionner `kibana/kibana_phytolex_export.ndjson`
5. Cliquer **Import**
6. Aller dans **Dashboard** → **PHYTOLEX - Dashboard Data Harmonisation**

**Option B — Via le terminal**

```bash
curl -X POST "http://localhost:5601/api/saved_objects/_import" \
  -H "kbn-xsrf: true" \
  --form file=@kibana/kibana_phytolex_export.ndjson
```

Puis ouvrir :

```
http://localhost:5601/app/dashboards#/view/phytolex-main-dashboard
```

---

## Dashboard — 8 visualisations

| # | Visualisation | Description |
|---|---|---|
| 1 | Taux d'alignement AGROVOC | Pie chart — % méthodes alignées vs non alignées |
| 2 | Top familles de méthodes | Bar chart — distribution par famille |
| 3 | Méthodes par concept AGROVOC | Bar chart — concepts les plus couverts |
| 4 | Répartition par satisfaction | Bar chart — niveaux de satisfaction |
| 5 | Répartition par secteur agricole | Bar chart — légumes, arboriculture, etc. |
| 6 | Familles et sous-familles | Bar groupé — drill-down famille/sous-famille |
| 7 | Détail complet des alignements | Data table — exploration interactive |
| 8 | Documents par source | Bar chart — répartition par source de données |

---

## Pipeline détaillé

```
                    ┌──────────────────┐
                    │   fetch_agrovoc   │  ← API REST agrovoc.fao.org
                    └────────┬─────────┘
                             │
┌──────────────────┐         │
│  normalize_dephy  │  ← XLSX local     │
└────────┬─────────┘         │
         │                   │
         ▼                   ▼
┌──────────────────────────────────────┐
│         normalize_agrovoc             │
└────────────────┬─────────────────────┘
                 │
                 ▼
         ┌──────────────┐
         │   dbt_seed    │  ← charge llm_synonyms.csv
         └──────┬───────┘
                │
                ▼
         ┌──────────────┐
         │   dbt_run     │  ← staging → mart_alignments
         └──────┬───────┘
                │
                ▼
       ┌────────────────┐
       │ export_parquet  │  ← DuckDB → Parquet enriched
       └──────┬─────────┘
              │
              ▼
       ┌──────────────┐
       │   index_es    │  ← Parquet → Elasticsearch
       └──────────────┘
```

---

## Data Lake — Convention de nommage

```
datalake/<layer>/<source>/<entity>/<YYYYMMDD>/fichier
```

| Layer | Contenu | Format |
|---|---|---|
| `raw/` | Données brutes telles que récupérées | JSON, XLSX |
| `formatted/` | Données nettoyées et normalisées | Parquet |
| `enriched/` | Données combinées et alignées | Parquet |

---

## Résultats

| Métrique | Valeur |
|---|---|
| Documents indexés | ~500 |
| Méthodes DEPHY distinctes | ~65 |
| Taux d'alignement AGROVOC | 83% |
| Extractions PDF par LLM | 380 |
| Secteurs agricoles couverts | 7 |

---

## Arrêter les services

```bash
docker compose down
```

Pour tout supprimer (volumes inclus) :

```bash
docker compose down -v
```