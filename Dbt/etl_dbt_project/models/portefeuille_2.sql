{{ config(materialized='table') }}

-- Step 1: Start from portefeuille
with source as (
    select * from {{ ref('portefeuille') }}
),

-- Step 2: Select rows to remove (by stripping the "rap_cin" column for comparison)
rows_to_remove as (
    select 
        sit,num_dos,n_dos,num_cin,nom_pre,dat_enr,csp,prd,sit_dos,stage,encours,prov,prv_cum,nbr_imp_initial,nbr_imp,age_imp,ech_deb,ech_fin,dur_crd,mt_cred,mensualite,mt_dit,cod_emp,lib_emp,cod_agc,lib_agc,dat_arr,commentaire,matricule,lib_vil,date_calcul,cod_rev,lib_rev,cli_adr,cli_vil,cli_tel,flag_pcx,csp_prd,mnt_echeance,mnt_imp,verif_bagatelle,flag_reechelonnement,ctx,spp,sinistre,segment,qualification

    from {{ ref('portefeuille_with_rap_cin') }}
),

-- Step 3: Subtract those rows from the base
base_client as (
    select * from source
    except
    select * from rows_to_remove
),

-- Step 4: Extract num_dos from DVR source
dvr_ids as (
    select distinct n_dos
    from {{ source('airflow', 'DVR') }}
),

-- Step 5: Add DVR flag to base_client
final_with_dvr as (
    select 
        bc.*,
        case 
            when d.n_dos is not null then 'DVR'
            else ''
        end as dvr
    from base_client bc
    left join dvr_ids d
        on bc.n_dos = d.n_dos
)

-- Step 6: Final output
select * from final_with_dvr
