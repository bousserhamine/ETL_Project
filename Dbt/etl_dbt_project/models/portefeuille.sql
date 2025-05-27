-- portefeuille.sql
{{ config(materialized='table') }}
select *
from {{ ref('portefeuille_base') }}
where csp_prd != 'EMPEF'