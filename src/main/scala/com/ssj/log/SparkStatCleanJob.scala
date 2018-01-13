package com.ssj.log

import org.apache.spark.sql.SparkSession


/**
  * Use spark to clean the data
  */
object SparkStatCleanJob {

    def main(args: Array[String]){

        val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

        //create an RDD of Rows from the original RDD
        val accessRDD = spark.sparkContext.textFile("file:///Users/shishengjia/data/access.log")

        val accessDF = spark.createDataFrame(accessRDD.map(x => ConvertUtil.parseLog(x)), ConvertUtil.struct)

        accessDF.printSchema()

        accessDF.show(false)

    }
}
