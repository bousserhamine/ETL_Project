-- portefeuille_base.sql

select *
from {{ ref('Full_Transformation') }}
where (qualification is null or qualification = '')
  and stage in ('S2', 'S3')
union
select *
from {{ ref('Full_Transformation') }}
where (qualification is null or qualification = '')
  and stage = 'S1'
  and csp_prd in ('Auto_CLASS', 'Auto_LOA', 'OPB_DISPO', 'Sogeconso', 'EMPEF')
  and nbr_imp::numeric >= 1 and age_imp::numeric >= 1
union
select *
from {{ ref('Full_Transformation') }}
where (qualification is null or qualification = '')
  and stage = 'S1'
  and csp_prd in ('FONCT', 'EMPHF')
  and nbr_imp::numeric >= 1
