package com.ssj.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkSessionApp {
  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("file:///Users/shishengjia/data/test.json")
    people.show()

    spark.stop()

  }
}
