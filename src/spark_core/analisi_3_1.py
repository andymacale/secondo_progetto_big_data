import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("ERRORE: path HDFS mancante")
        sys.exit(1)

    hdfs_path = sys.argv[1]

    perc = hdfs_path.split('_')[-1]

    spark = SparkSession.builder \
            .appName("SparkCore_3_1") \
            .getOrCreate()

    try:
        df = spark.read.option("mergeSchema", "true").parquet(hdfs_path)
        if df.rdd.isEmpty():
            raise Exception(f"La cartella {hdfs_path} e' vuota o non contiene Parquet validi!")

        ris = df.groupBy("op_unique_carrier", "origin", "month") \
                .agg(
                    F.count("*").alias("numero_voli"),
                    F.coalesce(F.min(
                        F.when((F.col("cancelled") == 0) & (F.col("arr_delay") >= 1.0), F.col("arr_delay"))
                    ), F.lit(0.0)).alias("ritardo_minimo"),
                    F.coalesce(F.max(
                        F.when(F.col("cancelled") == 0, F.col("arr_delay"))
                    ), F.lit(0.0)).alias("ritardo_massimo"),
                    F.round(F.avg(
                        F.greatest(F.lit(0), F.coalesce(F.col("arr_delay"), F.lit(0)))
                    ), 2).alias("ritardo_medio"),
                    F.round(
                        (F.sum(F.when(F.col("cancelled") == 1, 1.0).otherwise(0.0)) / F.count("*")) * 100.0, 2
                    ).alias("tasso_cancellazione")
                )

        final_df = ris.select(
            F.col("op_unique_carrier").alias("codice"),
            F.col("origin").alias("aeroporto_partenza"),
            F.col("numero_voli"),
            F.col("ritardo_minimo"),
            F.col("ritardo_massimo"),
            F.col("ritardo_medio"),
            F.col("tasso_cancellazione"),
            F.col("month").alias("mese")
        )

        output_dir = f"file:///home/andy/Documenti/secondo_progetto_big_data/results/spark_core_3_1_{perc}"
        
        final_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_dir)
                
        print(f"Salvataggio Spark completato in: {output_dir}")
    except Exception as e:
        print(f"ERRORE SparkCore_3_1 {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()