import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def setup_cassandra_session():
    """Establishes a connection with the Cassandra database."""
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info("Connected to Cassandra.")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

def configure_spark():
    """Configures and returns a Spark session."""
    try:
        spark = SparkSession.builder \
            .appName('RealtimeDataProcessing') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                                            'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session configured.")
        return spark
    except Exception as e:
        logging.error(f"Error configuring Spark session: {e}")
        return None

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    cassandra_session = setup_cassandra_session()
    if cassandra_session:
        spark = configure_spark()
        if spark:
            kafka_df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()

            schema = StructType([
                StructField("id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructIndex("last_name", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("address", StringType(), True),
                StructField("post_code", StringType(), True),
                StructField("email", StringType(), True),
                StructField("username", StringType(), True),
                StructField("registered_date", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("picture", StringType(), True)
            ])

            json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col('value'), schema).alias('data')).select("data.*")

            query = json_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option('checkpointLocation', '/tmp/checkpoint_dir') \
                .option('keyspace', 'spark_streams') \
                .option('table', 'users') \
                .start()

            query.awaitTermination()

if __name__ == "__main__":
    main()
