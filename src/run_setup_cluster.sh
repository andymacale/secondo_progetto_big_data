#!/bin/bash

PROJECT_DIR="$HOME/secondo_progetto_big_data"
SCRIPT_DIR="$PROJECT_DIR/src"
RESULT_CSV="$PROJECT_DIR/results_cluster/benchmark_results.csv"
DB_NAME="flights_project"
SQL_DIR="$SCRIPT_DIR/sql"

PERCENTUALI=("10" "25" "50" "75" "100" "150")
ANALISI=("3.1" "3.2" "3.3")

HIVE_SETTINGS="-hiveconf hive.exec.parallel=true -hiveconf hive.vectorized.execution.enabled=false -hiveconf hive.cli.print.header=true -hiveconf mapreduce.job.reduces=-1 -hiveconf hive.exec.reducers.bytes.per.reducer=33554432 -hiveconf hive.exec.reducers.max=8 -hiveconf hive.parquet.use.column.names=true -hiveconf parquet.column.index.access=false"
HIVE_LOGGER="--hiveconf hive.root.logger=WARN,console"

# Avvio e stop manuale disabilitati per Amazon EMR
echo ">>> Ambiente Hadoop gestito da EMR rilevato."

echo ">>> Pulizia HDFS e svuotamento CSV/Log locali..."
# Aggiornato il percorso HDFS per usare l'utente 'hadoop'
hdfs dfs -rm -r -f /user/hadoop/data/flights_[0-9]* 2>/dev/null

rm -f "$PROJECT_DIR/results_cluster"/*.csv 

echo "===================================================="
echo ">>> FASE 1: CARICAMENTO DATI DA LOCALE A HDFS"
echo "===================================================="
for PERC in "${PERCENTUALI[@]}"
do
    # Aggiornato il percorso HDFS per usare l'utente 'hadoop'
    CURRENT_PATH="/user/hadoop/data/flights_${PERC}"
    echo ">>> Preparazione $CURRENT_PATH..."
    
    hdfs dfs -mkdir -p "$CURRENT_PATH"
    
    hdfs dfs -put -f "$PROJECT_DIR/data/dataset_${PERC}.parquet" "$CURRENT_PATH/data.parquet"
    
    if [ $? -eq 0 ]; then
        echo "[OK] Caricato dataset_${PERC}.parquet"
    else
        echo "[ERRORE] Impossibile caricare dataset_${PERC}.parquet"
    fi
done


echo "===================================================="
echo ">>> FASE 2: ESECUZIONE BENCHMARK"
echo "===================================================="
echo "percentuale,motore,analisi,tempo_secondi,status" > "$RESULT_CSV"
for PERC in "${PERCENTUALI[@]}"
do
    echo "----------------------------------------------------"
    echo ">>> TEST VOLUME: ${PERC}% dei dati"
    echo "----------------------------------------------------"
    
    CURRENT_PATH="/user/hadoop/data/flights_${PERC}"

    echo ">>> Configurazione Tabella Hive..."
    hive $HIVE_LOGGER $HIVE_SETTINGS --hivevar path="$CURRENT_PATH" -f "$SQL_DIR/setup_table.sql" > /dev/null 2>&1

    for ANALISI_ID in "${ANALISI[@]}"
    do
        echo "Esecuzione Hive - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        hive $HIVE_LOGGER $HIVE_SETTINGS --hivevar db=$DB_NAME -f "$SQL_DIR/${ANALISI_ID}.sql" | sed 's/\t/,/g; s/'${DB_NAME}'\.//g' > "$PROJECT_DIR/results_cluster/hive_res_${ANALISI_ID}_${PERC}.csv" 2> "$SCRIPT_DIR/hive_error.log"
        echo "${PERC},Hive,${ANALISI_ID},$(( $(date +%s) - START )),$?" >> "$RESULT_CSV"

        echo "Esecuzione Spark SQL - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        spark-submit --master yarn --deploy-mode client "$SCRIPT_DIR/spark_sql/launcher.py" "$SQL_DIR/${ANALISI_ID}.sql" "$DB_NAME" | sed 's/\t/,/g' > "$PROJECT_DIR/results_cluster/spark_sql_${ANALISI_ID}_${PERC}.csv" 2> "$SCRIPT_DIR/spark_sql_error.log"
        echo "${PERC},Spark_SQL,${ANALISI_ID},$(( $(date +%s) - START )),$?" >> "$RESULT_CSV"

        echo "Esecuzione Spark Core - Analisi ${ANALISI_ID}..."
        START=$(date +%s)
        spark-submit --master yarn --deploy-mode client "$SCRIPT_DIR/spark_core/analisi_${ANALISI_ID//./_}.py" "$CURRENT_PATH"  2> "$SCRIPT_DIR/spark_core_error.log"
        echo "${PERC},Spark_Core,${ANALISI_ID},$(( $(date +%s) - START )),$?" >> "$RESULT_CSV"
        
        HDFS_RESULT_PATH="hdfs:///user/hadoop/results_spark_core_${ANALISI_ID//./_}_${PERC}"
        LOCAL_RESULT_PATH="$PROJECT_DIR/results_cluster/spark_core_${ANALISI_ID}_${PERC}.csv"
        
        hdfs dfs -getmerge "$HDFS_RESULT_PATH" "$LOCAL_RESULT_PATH" 2>/dev/null
        hdfs dfs -rm -r -skipTrash "$HDFS_RESULT_PATH" 2>/dev/null
    done
done

echo "Test completati. Risultati in $RESULT_CSV"