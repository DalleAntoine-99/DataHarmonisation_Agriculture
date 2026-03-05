{{ config(materialized='view') }}

with manual_mapping as (
  select
    dephy_term,
    lower(trim(dephy_term)) as dephy_term_normalized,
    matched_query,
    agrovoc_uri,
    agrovoc_label,
    agrovoc_id,
    agrovoc_concept_id,
    lang,
    vocab,
    source_file,
    formatted_at,
    data_layer
  from read_parquet('../datalake/formatted/agrovoc/concepts/{{ var("run_date") }}/part-00001.parquet')
),

llm_mapping as (
  select
    dephy_term,
    dephy_term_normalized,
    matched_query,
    agrovoc_uri,
    agrovoc_label,
    null as agrovoc_id,
    agrovoc_concept_id,
    'en'          as lang,
    'agrovoc'     as vocab,
    'llm_gemma3'  as source_file,
    formatted_at,
    data_layer
  from {{ ref('llm_synonyms') }}
)

select * from manual_mapping
union all
select * from llm_mapping