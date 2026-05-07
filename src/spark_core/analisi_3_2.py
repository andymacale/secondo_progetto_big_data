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
            .appName("SparkCore_3_2") \
            .getOrCreate()

    try:
        df = spark.read.option("mergeSchema", "true").parquet(hdfs_path)
        if df.rdd.isEmpty():
            raise Exception(f"La cartella {hdfs_path} e' vuota o non contiene Parquet validi!")

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

        cause_mapping = {
            'carrier_delay': 'Carrier',
            'weather_delay': 'Weather',
            'nas_delay': 'NAS',
            'security_delay': 'Security',
            'late_aircraft_delay': 'Late Aircraft'
        }
        
        cause_dfs = []

        for col_name, display_name in cause_mapping.items():
            riga = df.where("cancelled = 0").groupBy("origin", "month") \
                    .agg(F.sum(col_name).cast("int").alias("minuti_totali")) \
                    .withColumn("causa", F.concat(
                        F.lit(f"{display_name} ("), 
                        F.col("minuti_totali"), 
                        F.lit(" min)")
                    ))
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

        final_df_csv = final.select(
            F.col("origin").alias("aeroporto_partenza"),
            F.col("month").alias("mese"),
            "numero_ritardi_basso", 
            "numero_ritardi_medio",
            "numero_ritardi_alto", 
            F.concat(
                F.lit("['"), 
                F.concat_ws("', '", F.col("cause_maggiori")), 
                F.lit("']")
            ).alias("cause_maggiori")
        ).orderBy("aeroporto_partenza", "mese")


        #output_dir = f"file:///home/andy/Documenti/secondo_progetto_big_data/results/spark_core_3_2_{perc}"
        output_dir = f"hdfs:///user/hadoop/results_spark_core_3_2_{perc}"
        
        final_df_csv.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_dir)
                
        print(f"Salvataggio Spark completato in: {output_dir}")

    except Exception as e:
        print(f"ERRORE SparkCore_3_2: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()