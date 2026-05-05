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


if __name__ == "__main__":
    main()