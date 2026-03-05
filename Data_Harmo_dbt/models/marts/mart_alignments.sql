{{ config(materialized='table') }}

with dephy as (
  select * from {{ ref('stg_dephy') }}
),
agrovoc as (
  select * from {{ ref('stg_agrovoc') }}
),
align as (
  select
    d.*,
    a.agrovoc_label,
    a.agrovoc_concept_id,
    a.agrovoc_uri,
    a.matched_query,
    case when a.agrovoc_concept_id is not null then true else false end as is_aligned,
    {{ current_timestamp() }} as combined_at
  from dephy d
  left join agrovoc a
    on d.method_name_normalized = a.dephy_term_normalized
)

select * from align