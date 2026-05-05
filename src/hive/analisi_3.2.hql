use flights_project;

set hive.exec.parallel=true;
set mapreduce.job.reduces=4;
set hive.optimize.ppd=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.map.aggr=true;

with cause_ritardi as(

    select origin, month, 'Carrier' as causa, sum(carrier_delay) as minuti_totali
    from flights where cancelled = 0 group by origin, month

    union all

    select origin, month, 'Weather' as causa, sum(wheather_delay) as minuti_totali
    from flights where cancelled = 0 group by origin, month

    union all
    
    select origin, month, 'NAS' as causa, sum(nas_delay) as minuti_totali
    from flights where cancelled = 0 group by origin, month

    union all
    
    select origin, month, 'Security' as causa, sum(security_delay) as minuti_totali
    from flights where cancelled = 0 group by origin, month

    union all

    select origin, month, 'Late Aircraft' as causa, sum(late_aircraft_delay) as minuti_totali
    from flights where cancelled = 0 group by origin, month
),
classifica_cause as (
    select origin, month, causa, minuti_totali
        row_number() over(partition by origin, month order by minuti_totali desc) as ranking
    from cause_ritardi
)

-- row number assegna un numero progressivo (1, 2, 3...) a ogni riga

select f.origin as aeroporto_partenza,
       f.month as mese,
       sum(case when f.cancelled = 0 and f.dep_delay < 15.0 then 1 else 0 end) as numero_ritardi_basso,
       sum(case when f.cancelled = 0 and f.dep_delay between 15.0 and 60.0 then 1 else 0 end) as numero_ritardi_medio,
       sum(case when f.cancelled = 0 and f.dep_delay > 60.0 then 1 else 0 end) as numero_ritardo_alto,
       collect_list(case when c.ranking <= 3 then concat(c.causa, ' (', c.minuti_totali, ' min')) end) as cause_maggiori
from flights f
left join classifica_cause c on f.origin = c.origin and f.month = c.month
group by origin, month