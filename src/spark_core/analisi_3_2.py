import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

def main():
    if len(sys.argv) < 2:
        print("ERRORE: path HDFS mancante")
        sys.exit(1)

    hdfs_path = sys.argv[1]

    spark = SparkSession.builder \
            .appName("SparkCore_3_2") \
            .getOrCreate()

    try:
        df = spark.read.parquet(hdfs_path)

        conta = df.groupBy("origin", "month").agg(
                F.sum(
                    F.when((F.col("cancelled") == 0) & (F.col("dep_delay") < 15.0), 1) \
                    .otherwise(0)).alias("numero_ritardi_basso"),
                F.sum(
                    F.when((F.col("cancelled") == 0) & (F.col("dep_delay").between(15.0, 60.0)), 1) \
                    .otherwise(0)).alias("numero_ritardi_medio"),
                F.sum(
                    F.when((F.col("cancelled") == 0) & (F.col("dep_delay") > 60.0), 1) \
                    .otherwise(0)).alias("numero_ritardi_alto"),       
        )

        cause = ['carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
        cause_dfs = []

        for causa in cause:
            riga = df.where("cancelled = 0").groupBy("origin", "month") \
                    .agg(F.sum(causa).alias("minuti_totali")) \
                    .withColumn("causa", F.lit(causa))
            cause_dfs.append(riga)

        unione = cause_dfs[0]
        for pross_df in cause_dfs[1:]:
            unione = unione.unionAll(pross_df)

        window_spec = Window.partitionBy("origin", "month").orderBy(F.col("minuti_totali").desc())
        ranked = unione.withColumn("ranking", F.row_number().over(window_spec)) \
                    .where("ranking <= 3") \
                    .groupBy("origin", "month") \
                    .agg(F.collect_list("causa").alias("cause_maggiori"))

        final = conta.join(ranked, ["origin", "month"], "left")

        final_df = final.select(
            F.col("origin").alias("aeroporto_partenza"),
            F.col("month").alias("mese"),
            "numero_ritardi_basso", 
            "numero_ritardi_medio",
            "numero_ritardi_alto", 
            "cause_maggiori"
        )

        final_df.collect()

    except Exception as e:
        print(f"ERRORE SparkCore_3_2: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()