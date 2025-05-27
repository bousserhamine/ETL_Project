{{ config(materialized='table') }}

-- Step 1: Load client data and join with flag_pcx
with base_client as (
    select * 
    from {{ source('airflow', 'new_base') }}
),

flag_pcx as (
    select distinct num_dos
    from {{ source('airflow', 'flag_pcx') }}
),

flag_pcx_output as (
    select
        base_client.*,
        case 
            when flag_pcx.num_dos is not null then 'PCX'
            else ''
        end as flag_pcx
    from base_client
    left join flag_pcx
        on base_client.num_dos = flag_pcx.num_dos
),

-- Step 2: Add csp_prd logic
sogeconso as (
    select distinct num_dos
    from {{ source('airflow', 'sogeconso') }}
),

csp_prd as (
    select
        flag_pcx_output.*,
        case
            when sogeconso.num_dos is not null then 'Sogeconso'
            when flag_pcx = 'PCX' then 'EMPHF'
            when PRD = 'L' then 'Auto_LOA'
            when PRD = 'A' then 'Auto_CLASS'
            when PRD in ('M', 'D') and CSP = 'V' then 'OPB_DISPO'
            when PRD in ('M', 'D') and CSP in ('C', 'F') then 'FONCT'
            when PRD in ('M', 'D') and CSP in ('E', 'I', 'R') then 'EMPEF'
            else null
        end as csp_prd
    from flag_pcx_output
    left join sogeconso
        on flag_pcx_output.num_dos = sogeconso.num_dos
),

-- Step 3: Add mnt_echeance / mnt_imp / Bagatelle / flag_reechelonnement
bag_reech as (
    select
        csp_prd.*,
        coalesce(mensualite::numeric, 0) + coalesce(mt_dit::numeric, 0) as mnt_echeance,
        (coalesce(mensualite::numeric, 0) + coalesce(mt_dit::numeric, 0)) * coalesce(nbr_imp::numeric, 0) as mnt_imp,
        case 
            when encours::numeric < (coalesce(mensualite::numeric, 0) + coalesce(mt_dit::numeric, 0)) 
            then 'Bagatelle'
            else 'IMP'
        end as verif_bagatelle,
        case 
            when cod_agc = '102' then 'Oui'
            else ''
        end as flag_reechelonnement
    from csp_prd
),

-- Step 4: Join with CTX, SPP, Sinistre
ctx as (
    select distinct num_dos
    from {{ source('airflow', 'ctx') }}
),
spp as (
    select distinct num_dos
    from {{ source('airflow', 'spp') }}
),
sinistre as (
    select distinct num_dos
    from {{ source('airflow', 'siniste') }}
),

flag_ctx_spp_sinistre as (
    select
        bag_reech.*,
        case when ctx.num_dos is not null then 'CTX' else '' end as ctx,
        case when spp.num_dos is not null then 'SPP' else '' end as spp,
        case when sinistre.num_dos is not null then 'Sinistre' else '' end as sinistre
    from bag_reech
    left join ctx on bag_reech.num_dos = ctx.num_dos
    left join spp on bag_reech.num_dos = spp.num_dos
    left join sinistre on bag_reech.num_dos = sinistre.num_dos
),

-- Step 5: Final segmentation logic
final_segment as (
    select
        flag_ctx_spp_sinistre.*,
        case
            when csp_prd in ('Auto_CLASS', 'Auto_LOA') then
                case
                    when num_cin ~* '^(IF|RC)' then 'PM'
                    when num_cin ~ '^\d{4,}$' then 'PM'
                    else 'PP'
                end
            else csp_prd
        end as segment
    from flag_ctx_spp_sinistre
),

-- Step 6: Add qualification logic
final_qualified as (
    select
        *,
        case
            when sit = 'SLD' then 'SLD'
            when encours::numeric = 0 then 'Enc=0'
            when nbr_imp::numeric = 0 then 'nbr_imp=0'
            when spp = 'SPP' then 'SPP'
            when sinistre = 'Sinistre' then 'Sinistre'
            when ctx = 'CTX' then 'CTX'
            when encours::numeric < 600 and verif_bagatelle = 'Bagatelle' then 'Bagatelle<600'
            when csp_prd in ('Auto_CLASS', 'Auto_LOA', 'OPB_DISPO', 'Sogeconso', 'EMPEF')
                 and nbr_imp::numeric = 1
                 and age_imp::numeric = 0 then 'Tombe'
            else ''
        end as qualification
    from final_segment
)

-- Final output
select * from final_qualified
