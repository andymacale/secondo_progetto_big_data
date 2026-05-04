use flights_project;

set hive.exec.parallel=true;
set mapreduce.job.reduces=4; -- Sfruttiamo i core del tuo Victus

select 
    op_unique_carrier as vettore,
    origin as aeroporto_partenza,
    count(*) as totale_voli,
    round(avg(arr_delay), 2) as ritardo_medio_arrivo,
    sum(case when arr_delay > 15 then 1 else 0 end) AS voli_in_ritardo_grave
from 
    flights
where 
    cancelled = 0 and diverted = 0
group by 
    op_unique_carrier, 
    origin
having 
    totale_voli > 100 
order by 
    ritardo_medio_arrivo desc;