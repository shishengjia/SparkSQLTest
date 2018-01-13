package com.ssj.spark

import org.apache.spark.sql.SparkSession


/**
  * extract the information we need from the file
  */
object SparkStatFormatJob {

    def main(args: Array[String]) {

        val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

        val access = spark.sparkContext.textFile("file:///Users/shishengjia/data/10000_access.log")

        access.map(line => {

            // split each line by " "
            val splits = line.split(" ")

            // 10.100.0.1
            val ip = splits(0)

            // [10/Nov/2016:00:01:02 +0800]
            val time = splits(3) + " " + splits(4)

            val url = splits(11).replaceAll("\"", "")

            val traffic = splits(9)

            DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip

        }).saveAsTextFile("file:///Users/shishengjia/data/output/")

        spark.stop()
    }
}
