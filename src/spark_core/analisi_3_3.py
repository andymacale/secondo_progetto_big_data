import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


def main():
    if len(sys.argv) < 2:
        print("ERRORE: path HDFS mancante")
        sys.exit(1)

    hdfs_path = sys.argv[1]

    perc = hdfs_path.split('_')[-1]

    spark = SparkSession.builder \
            .appName("SparkCore_3_3") \
            .getOrCreate()

    try:
        df = spark.read.option("mergeSchema", "true").parquet(hdfs_path)
        
        if df.rdd.isEmpty():
            raise Exception(f"La cartella {hdfs_path} e' vuota o non contiene Parquet validi!")

        aeroporto_stat = df.groupBy("origin") \
            .agg(F.round(F.avg(F.when(F.col("cancelled") == 0, F.col("dep_delay"))), 2).alias("ritardo_medio_complessivo"))

        aeroporto_compagnia_stat = df.groupBy("origin", "op_unique_carrier") \
            .agg(
                F.count("*").alias("numero_voli"),
                F.round(F.avg(F.when(F.col("cancelled") == 0, F.col("dep_delay"))), 2).alias("ritardo_medio_partenza"),
                F.round(F.avg(F.when(F.col("cancelled") == 0, F.col("arr_delay"))), 2).alias("ritardo_medio_arrivo"),
                F.round((F.sum(F.when(F.col("cancelled") == 1, 1.0).otherwise(0.0)) / F.count("*")) * 100.0, 2).alias("tasso_cancellazione")
            )

        window_spec = Window.partitionBy("aeroporto_partenza").orderBy("ritardo_medio_partenza")

        final_result = aeroporto_compagnia_stat.join(
            aeroporto_stat, 
            aeroporto_compagnia_stat.origin == aeroporto_stat.origin
        ).select(
            aeroporto_compagnia_stat.origin.alias("aeroporto_partenza"),
            F.col("op_unique_carrier").alias("compagnia"),
            "numero_voli",
            "ritardo_medio_partenza",
            "ritardo_medio_arrivo",
            "tasso_cancellazione",
            F.round(F.col("ritardo_medio_partenza") - F.col("ritardo_medio_complessivo"), 2).alias("differenza")
        ).withColumn("classifica", F.rank().over(window_spec))

        output_dir = f"file:///home/andy/Documenti/secondo_progetto_big_data/results/spark_core_3_3_{perc}"
        
        final_result.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_dir)
                
        print(f"Salvataggio Spark completato in: {output_dir}")

    except Exception as e:
        print(f"ERRORE SparkCore_3_3: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()