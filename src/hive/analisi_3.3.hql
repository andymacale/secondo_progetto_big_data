use flights_project;

set hive.exec.parallel=true;
set mapreduce.job.reduces=4;
set hive.optimize.ppd=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.map.aggr=true;

with aeroporto_stat as(
    select origin,
           round((avg(case when cancelled = 0 then dep_delay end)), 2) as ritardo_medio_complessivo
    from flights
    group by origin
),
aeroporto_compagnia_stat as (
    select origin as aeroporto_partenza, op_unique_carrier as compagnia,
    count(*) as numero_voli,
    round(avg(case when cancelled = 0 then dep_delay end), 2) as ritardo_medio_partenza,
    round(avg(case when cancelled = 0 then arr_delay end), 2) as ritardo_medio_arrivo,
    round((sum(case when cancelled = 1 then 1.0 else 0.0)/count(*))*100.0, 2) as tasso_cancellazione
    from flights
    group by origin, op_unique_carrier
)

select ac.aeroporto_partenza,
       ac.compagnia,
       ac.numero_voli,
       ac.ritardo_medio_partenza,
       ac.ritardo_medio_arrivo,
       ac.tasso_cancellazione,
       ac.ritardo_medio_partenza - a.ritardo_medio_complessivo as differenza,
       rank() over(partition by ac.aeroporto_partenza order by ac.ritardo_medio_partenza asc) as classifica
from aeroporto_compagnia_stat ac
join aeroporto_stat a on ac.aeroporto_partenza = a.origin