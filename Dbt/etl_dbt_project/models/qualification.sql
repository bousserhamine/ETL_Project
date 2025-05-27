-- models/transformations/add_qualification.sql
{{ config(materialized='table') }}
with base as (
    select * from {{ source('airflow','base') }}
),

qualified as (
    select
        *,
        case
            when sit = 'SLD' then 'SLD'
            when encours = 0 then 'Enc=0'
            when nbr_imp = 0 then 'nbr_imp=0'
            when spp = 'SPP' then 'SPP'
            when sinistre = 'Sinistre' then 'Sinistre'
            when ctx = 'CTX' then 'CTX'
            when encours < 600 and verif_bagatelle = 'Bagatelle' then 'Bagatelle<600'
            when csp_prd in ('Auto_CLASS', 'Auto_LOA', 'OPB_DISPO', 'Sogeconso', 'EMPEF')
                 and nbr_imp = 1
                 and age_imp = 0 then 'Tombe'
            else ''
        end as qualification
    from base
)

select * from qualified

