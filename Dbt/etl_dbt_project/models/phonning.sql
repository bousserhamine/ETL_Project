-- models/phonning.sql

{{ config(materialized='table') }}

with source as (
    select * from {{ ref('portefeuille_2') }}
),

sogeconso_filter as (
    select *
    from source
    where csp_prd = 'Sogeconso'
      and nbr_imp::numeric <= 3
),

fonct_filter as (
    select *
    from source
    where csp_prd = 'FONCT'
      and nbr_imp::numeric <= 5
      and lib_vil != 'FES'
),


segment_pm_filter as (
    select *
    from source
    where segment = 'PM'
      and nbr_imp::numeric < 3 
),

segment_pp_filter as (
    select *
    from source
    where segment = 'PP'
      and nbr_imp::numeric <= 5
),

opb_emphf_filter as (
    select *
    from source
    where csp_prd IN ('OPB_DISPO', 'EMPHF')
      and nbr_imp::numeric <= 5
)

select * from sogeconso_filter
union all
select * from fonct_filter
union all
select * from segment_pm_filter
union all
select * from segment_pp_filter
union all
select * from opb_emphf_filter
