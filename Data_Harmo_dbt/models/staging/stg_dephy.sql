{{ config(materialized='view') }}

with src as (
  select *
  from read_parquet('../datalake/formatted/dephy/variableCards/20260222/part-00001.parquet')
)

select
  method_name,
  lower(trim(method_name)) as method_name_normalized,
  family,
  sub_family,
  treatment_type,
  treated_part,
  mode_of_action,
  target_group,
  main_target,
  other_characteristics,
  application_stage,
  rdd_or_oad,
  application_count,
  application_dose,
  satisfaction_level,
  sector,
  crops,
  outdoor_or_indoor,
  experiment_period,
  project_name,
  experimental_site,
  site_postal_code,
  system_name,
  system_link,
  source_file,
  formatted_at,
  data_layer
from src