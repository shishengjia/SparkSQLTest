package com.ssj.spark

import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

/**
  * Interoperation between DataFrame and RDD
  */

object DataFrameRDDApp {

  def main(args: Array[String]){

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    program(spark)

    spark.stop()
  }

  def program(spark: SparkSession){
    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/shishengjia/data/infos.txt")

    //create an RDD of Rows from the original RDD
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    //create the schema represented by a StructType matching the structure of Rows in the RDD created above
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    //apply the schema to the RDD of Rows
    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()
  }


  def reflection(spark: SparkSession){
    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/shishengjia/data/infos.txt")

    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDF.show()

    //then you can use DataFrame API to operate the file

    //way 1
    infoDF.filter(infoDF.col("age") > 30).show()

    //way 2
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
    spark
  }

  case class Info(id: Int, name: String, age: Int)
}
