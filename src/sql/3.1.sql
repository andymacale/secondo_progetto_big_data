use ${db};
select
    op_unique_carrier as codice,
    origin as aeroporto_partenza,
    count(*) as numero_voli,
    min(case when cancelled = 0 and arr_delay >= 1.0 then arr_delay end) as ritardo_minimo,
    max(case when cancelled = 0 then arr_delay end) as ritardo_massimo,
    round(avg(greatest(0, coalesce(arr_delay, 0)) end), 2) as ritardo_medio,
    round((sum(case when cancelled = 1 then 1.0 else 0.0)/count(*))*100.0, 2) as tasso_cancellazione,
    month as mese
from flights
group by op_unique_carrier, origin, month