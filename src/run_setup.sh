#!/bin/bash

source ~/.bashrc

PERCENTUALI=("10" "25" "50" "75" "100" "150")
ANALISI=("3.1" "3.2" "3.3")
DB_NAME="flights_project"
RESULT_CSV="../data/benchmark_results.csv"
SQL_DIR="../sql"

HIVE_SETTINGS="-hiveconf hive.exec.parallel=true -hiveconf mapreduce.job.reduces=4 -hiveconf hive.vectorized.execution.enabled=true -hiveconf hive.vectorized.execution.reduce.enabled=true"

trap stop_hadoop EXIT

echo ">>> Avvio ambiente Hadoop..."
start_hadoop


if [ ! -f "$RESULT_CSV" ]; then
    echo "percentuale,motore,analisi,tempo_secondi,status" > "$RESULT_CSV"
fi

for PERC in "${PERCENTUALI[@]}"
do
    echo "===================================================="
    echo ">>> TEST VOLUME: ${PERC}% dei dati"
    echo "===================================================="
    
    CURRENT_PATH="/user/andy/data/flights_${PERC}"

    echo ">>> Preparazione Tabella Hive per volume ${PERC}%..."
    hive $HIVE_SETTINGS --hivevar db=$DB_NAME --hivevar path=$CURRENT_PATH \
         -e "drop table if exists ${DB_NAME}.flights; $(cat $SQL_DIR/setup_table.sql)" > /dev/null 2>&1

    for ANALISI_ID in "${ANALISI[@]}"
    do
        echo "Esecuzione Hive - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        hive $HIVE_SETTINGS -f "$SQL_DIR/${ANALISI_ID}.sql" > /dev/null 2>&1
        STATUS=$?
        END=$(date +%s)
        echo "${PERC},Hive,${ANALISI_ID},$((END - START)),${STATUS}" >> "$RESULT_CSV"

        
        echo "Esecuzione Spark SQL - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        spark-submit --master local[*] ./spark_sql/launcher.py "$SQL_DIR/${ANALISI_ID}.sql" "$DB_NAME" > /dev/null 2>&1
        STATUS=$?
        END=$(date +%s)
        echo "${PERC},Spark_SQL,${ANALISI_ID},$((END - START)),${STATUS}" >> "$RESULT_CSV"

        
        echo "Esecuzione Spark Core - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        spark-submit --master local[*] "./spark_core/analisi_${ANALISI_ID//./_}.py" "$CURRENT_PATH" > /dev/null 2>&1
        STATUS=$?
        END=$(date +%s)
        echo "${PERC},Spark_Core,${ANALISI_ID},$((END - START)),${STATUS}" >> "$RESULT_CSV"
    done
done

echo "Tutti i test sono completati. Risultati in $RESULT_CSV"