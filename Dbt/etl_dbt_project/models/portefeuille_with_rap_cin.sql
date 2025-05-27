-- models/portefeuille_with_rap_cin.sql
{{ config(materialized='table') }}

with portefeuille as (
    select *
    from {{ ref('portefeuille') }}
),

brigade as (
    select distinct num_cin as brigade_cin
    from {{ source('airflow','Brigade_M_1') }}
),

joined as (
    select
        p.*,
        case 
            when b.brigade_cin is not null then b.brigade_cin
            else null
        end as rap_cin
    from portefeuille p
    left join brigade b
        on p.num_cin = b.brigade_cin
)

select *
from joined
where rap_cin is not null and rap_cin != ''
