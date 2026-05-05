import sys
import os
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 3:
        print("ERRORE: numero di parametri insufficienti")
        print("Utilizzo: spark_sql_launcher.py <percorso_file_sql> <nome_db>")
        sys.exit(1)
    
    sql_path = sys.argv[1]
    db_name = sys.argv[2]

    if not os.path.exists(sql_path):
        print(f"ERRORE: Il file SQL '{sql_path}' non esiste.")
        sys.exit(1)


    try:
        spark = SparkSession.builder \
                .appName(f"Benchmark_SparkSQL_{os.path.basename(sql_path)}") \
                .enableHiveSupport() \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()

        spark.sql(f"use {db_name}")

        try:
            with open(sql_path, 'r') as file:
                query = file.read()
        except Exception as e:
            print(f"ERRORE durante la lettura del file: {e}")
            sys.exit(1)

        try:
            print(f"Esecuzione query da: {sql_path}...")
            spark.sql(query).collect()
        except Exception as e:
            print(f"ERRORE durante l'esecuzione della query SQL: {e}")
            sys.exit(1)

    except Exception as e:
        print(f"ERRORE durante l'inizializzazione di Spark: {e}")
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()