# Import Libraries
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import to_json
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.functions import unix_timestamp
import logging 

#****************************************************************************************

# Set Logging
logging.basicConfig(filename = "/home/sara/Desktop/My_Project/Stream_Stock_Prediction/Stream_Predict.log",
                   level = logging.INFO,
                   format = '%(asctime)s :: %(levelname)s :: %(message)s',
                   datefmt = '%Y-%m-%d %H:%M:%S'
                   )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#****************************************************************************************

# Initialize Spark Session
spark = SparkSession.builder.appName("StreamPredict").getOrCreate()
logger.info(f"SparkSession build successfully.\n-------------------------------------------------------")

#****************************************************************************************

# Read real-time data from Kafka topic
rawData = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Stock") \
    .option("startingOffsets", "earliest") \
    .load()
logger.info(f"Read data from topic successfully.\n-------------------------------------------------------")

#****************************************************************************************

# Define the schema for the JSON data 
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

#****************************************************************************************

# Load the saved machine learning model
model_path = "file:///tmp/model/LinearRegression"
saved_model = PipelineModel.load(model_path)
logger.info(f"Load model successfully.\n-------------------------------------------------------")

#****************************************************************************************

# Extract kafka topic data to spark dataframe
parsed_stream = rawData.selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_timestamp") \
    .select(from_json("value", schema).alias("data"), "kafka_timestamp") \
    .select("data.*", "kafka_timestamp")
    
# Conver kafka timestamp to unix_timestamp for filtering in dashboard as double type
parsed_stream = parsed_stream.withColumn(
    "kafka_Time", 
    unix_timestamp(col("kafka_Timestamp"), "yyyy-MM-dd HH:mm:ss").cast("double")
)
# Drop kafka_timestamp
parsed_stream = parsed_stream.drop("kafka_timestamp")

# Select columns (matches PostgreSQL table schema)
parsed_stream = parsed_stream.select(
    col("date").alias("date_A"),  
    col("open").alias("open_A"),
    col("high").alias("high_A"),
    col("low").alias("low_A"),
    col("close").alias("close_A"),
    col("volume").alias("volume_A"),
    "kafka_Time"
)
logger.info(f"Parsed stream successfully.\n-------------------------------------------------------")

#****************************************************************************************

# Define a VectorAssembler to assemble features
feature_columns = ["high_A", "low_A", "close_A", "volume_A"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="AA_features")
parsed_stream_ = assembler.transform(parsed_stream)

# Make predictions using the loaded model
predictions = saved_model.transform(parsed_stream_)
logger.info(f"Predict successfully.\n-------------------------------------------------------")

#****************************************************************************************

# Define your PostgreSQL properties
postgres_properties = {
    "user": "user",
    "password": "Password",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://localhost:5432/postgres",
    "dbtable": "DailyStock"
}

#****************************************************************************************

def predict_and_save_to_postgresql(batch_df, batch_id):
    try:
        # Select the required columns and exclude the "prediction" column
        columns_to_write = ["date_A", "open_A", "high_A", "low_A", "close_A", "volume_A", "kafka_Time","prediction"]
        batch_df = batch_df.select(columns_to_write)

        # Write mode is "append" to insert new data
        batch_df.write.mode("append").jdbc(
            url=postgres_properties["url"],
            table=postgres_properties["dbtable"],
            properties=postgres_properties
        )
        print("Successfully wrote batch to PostgreSQL.")
    except Exception as e:
        print("Error writing batch to PostgreSQL:", str(e))

#****************************************************************************************

# Define your actual DataFrame
stream = predictions.writeStream \
    .outputMode("update") \
    .foreachBatch(predict_and_save_to_postgresql) \
    .start()

stream.awaitTermination()
