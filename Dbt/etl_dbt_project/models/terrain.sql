{{ config(materialized='table') }}

with source as (
    select * from {{ ref('portefeuille_2') }}
),

phonning_filter as (
    select sit,num_dos,n_dos,num_cin,nom_pre,dat_enr,csp,prd,sit_dos,stage,encours,prov,prv_cum,nbr_imp_initial,nbr_imp,age_imp,ech_deb,ech_fin,dur_crd,mt_cred,mensualite,mt_dit,cod_emp,lib_emp,cod_agc,lib_agc,dat_arr,commentaire,matricule::text,lib_vil,date_calcul,cod_rev,lib_rev,cli_adr,cli_vil,cli_tel,flag_pcx,csp_prd,mnt_echeance,mnt_imp,verif_bagatelle,flag_reechelonnement,ctx,spp,sinistre,segment,qualification,code_resultat
    from {{ source('airflow','phonning_m-1') }}
    where code_resultat in ('INJTL', 'INSTL', 'REFTL')
      and nbr_imp::numeric between 3 and 5
),

sogeconso_filter as (
    select *
    from source
    where csp_prd = 'Sogeconso'
      and nbr_imp::numeric > 3
),

fonct_filter as (
    select *
    from source
    where csp_prd = 'FONCT'
      and nbr_imp::numeric > 5
),

segment_pm_filter as (
    select *
    from source
    where segment = 'PM'
      and nbr_imp::numeric between 3 and 5
),

segment_pp_filter as (
    select *
    from source
    where segment = 'PP'
      and nbr_imp::numeric > 5
      and nbr_imp::numeric <= 6
),

opb_emphf_filter as (
    select *
    from source
    where csp_prd IN ('OPB_DISPO', 'EMPHF')
      and nbr_imp::numeric > 5
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
union all
select 
    sit,
    num_dos,
    n_dos,
    num_cin,
    nom_pre,
    to_date(dat_enr, 'DD/MM/YYYY') as dat_enr,
    csp,
    prd,
    sit_dos,
    stage,
    encours::text,
    prov::text,
    prv_cum::text,
    nbr_imp_initial::text,
    nbr_imp::text,
    age_imp::text,
    to_date(ech_deb, 'DD/MM/YYYY') as ech_deb,
    to_date(ech_fin, 'DD/MM/YYYY') as ech_fin,
    dur_crd::text,
    mt_cred::text,
    mensualite::text,
    mt_dit::text,
    cod_emp,
    lib_emp,
    cod_agc::text,
    lib_agc,
    to_date(dat_arr, 'DD/MM/YYYY') as dat_arr,
    commentaire,
    matricule::text,
    lib_vil,
    to_date(date_calcul, 'DD/MM/YYYY') as date_calcul,
    cod_rev,
    lib_rev,
    cli_adr,
    cli_vil::text,
    cli_tel::text,
    flag_pcx,
    csp_prd,
    mnt_echeance,
    mnt_imp,
    verif_bagatelle,
    flag_reechelonnement,
    ctx::text,
    spp::text,
    sinistre::text,
    segment,
    qualification::text,
    code_resultat
from phonning_filter
