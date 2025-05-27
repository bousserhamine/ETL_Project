{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('portefeuille_with_rap_cin') }}
),

segment_pm_filter as (
    select *
    from {{ ref('portefeuille_2') }}
    where segment = 'PM' and nbr_imp::numeric > 5
),

segment_pp_filter as (
    select *
    from {{ ref('portefeuille_2') }}
    where segment = 'PP' and nbr_imp::numeric > 6
),

filtered_portefeuille_2 as (
    select * from segment_pm_filter
    union all
    select * from segment_pp_filter
)

select * from filtered_portefeuille_2
