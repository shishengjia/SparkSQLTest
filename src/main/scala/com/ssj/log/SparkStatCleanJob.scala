package com.ssj.log

import com.ssj.log.utils.ConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Use spark to clean the data
  */
object SparkStatCleanJob {

    def main(args: Array[String]){

        val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

        //create an RDD of Rows from the original RDD
        val accessRDD = spark.sparkContext.textFile("file:///Users/shishengjia/data/access.log")

        val accessDF = spark.createDataFrame(accessRDD.map(x => ConvertUtil.parseLog(x)), ConvertUtil.struct)

//        accessDF.printSchema()
//
//        accessDF.show(false)

        accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).
          partitionBy("day").save("/Users/shishengjia/data/clean")

    }
}
