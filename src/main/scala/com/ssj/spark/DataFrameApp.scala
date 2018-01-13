package com.ssj.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API basic operations
  */

object DataFrameApp {
  def main(args: Array[String]){

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //load json file to dataframe
    val peopleDF  =spark.read.format("json").load("file:///Users/shishengjia/data/test.json")

    //print dataframe's schema information (tree view)
    peopleDF.printSchema()

    //default 20
    peopleDF.show()

    //like select name from table
    peopleDF.select("name").show()

    // like select name, nums+10 as nums2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("nums") + 10).as("nums2")).show()

    //like select * from table where nums > 19
    peopleDF.filter(peopleDF.col("nums") > 19).show()

    //liek select nums, count(1) from table group by nums
    peopleDF.groupBy("nums").count().show()

    spark.stop()
  }
}
