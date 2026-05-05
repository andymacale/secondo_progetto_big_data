#!/bin/bash
DB_NAME="flights_project"
RESULT_CSV="../data/benchmark_results.csv"

SQL_DIR="../sql"

HIVE_SETTINGS="-hiveconf hive.exec.parallel=true -hiveconf mapreduce.job.reduces=4 -hiveconf hive.vectorized.execution.enabled=true -hiveconf hive.vectorized.execution.reduce.enabled=true"

if [ ! -f "$RESULT_CSV" ]; then
    echo "percentuale,motore,analisi,tempo_secondi,status" > "$RESULT_CSV"
fi