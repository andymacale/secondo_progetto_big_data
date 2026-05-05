import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    if len(sys.argv) < 2:
        print("ERRORE: path HDFS mancante")
        sys.exit(1)

    hdfs_path = sys.argv[1]

    spark = SparkSession.builder \
            .appName("SparkCore_3_1") \
            .getOrCreate()

    df = spark.read.parquet(hdfs_path)

   
    ris = df.groupBy("op_unique_carrier", "origin", "month") \
            .agg(
                F.count("*").alias("numero_voli"),
                F.min(
                    F.when((F.col("cancelled") == 0) & (F.col("arr_delay") >= 1.0), F.col("arr_delay"))
                ).alias("ritardo_minimo"),
                F.max(
                    F.when(F.col("cancelled") == 0, F.col("arr_delay"))
                ).alias("ritardo_massimo"),
                F.round(F.avg(
                    F.when(F.col("cancelled") == 0, 
                           F.greatest(F.lit(0), F.coalesce(F.col("arr_delay"), F.lit(0))))
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

    final_df.collect()
    spark.stop()

if __name__ == "__main__":
    main()