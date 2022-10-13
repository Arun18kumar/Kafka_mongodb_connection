package Kafka
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Struct

object Mongokaf {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoDB")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.schemaInterference","true")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","top3")
      .option("startingOffsets","earliest")
      .option("failOnDataLoss","false")
      .load()

      df.printSchema()

    val schema = StructType(List(StructField("id", StringType),StructField("namval jsondf = df.select(from_json(col("value").cast(StringType),schema).alias("value"))
    jsondf.printSchema()e",StringType)))

    val valueDF = df.select(from_json(col("value").cast("string"),schema).alias("value"))

//    valueDF.printSchema()

    val finalDF = valueDF.select("value.*")

//    finalDF.printSchema()
//    val sdf = df.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
//
//    val fdf = sdf.select(from_json(col("value"),schema)).alias("student")
//
//    fdf.printSchema()
//
//    val finaldf = fdf.select(col("students.*"))
//
//    finaldf.printSchema()
//
//    val makedf = finaldf.select(col("from_json")).as("newstu")
//
//    makedf.printSchema()

    val outputQuery = finalDF.writeStream
      .foreachBatch(writetomongo _)
      .outputMode("append")
      .option("checkpointLocation","chk-point-dir")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    outputQuery.awaitTermination()
  }
  def writetomongo(df: DataFrame, batchID : Long): Unit ={
    df.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("checkpoint","/tmp")
      .option("forceDeleteTempCheckpointLocation", "true")
//      .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1/test.myCollection")
      .option("database", "test")
      .option("collection", "myCollection")
      .mode("append")
      .save()
    df.printSchema()
  }
}
