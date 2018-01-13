package com.ssj.spark

import org.apache.spark.sql.SparkSession

object DatasetApp {

  def main(args: Array[String]){
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    val path = "file:///Users/shishengjia/data/sales.csv"

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    import spark.implicits._

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show()

  }


  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
}
