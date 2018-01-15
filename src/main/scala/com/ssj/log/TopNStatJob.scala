package com.ssj.log

import com.ssj.log.dao.StatDAO
import com.ssj.log.model.{DayCityVideoAccess, DayVideoAccess}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN course
  */

object TopNStatJob {

    def main(args: Array[String]){

        val spark = SparkSession.builder().appName("TopNStatJob")
          .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
          .master("local[2]").getOrCreate()

        val accessDF = spark.read.format("parquet").load("/Users/shishengjia/data/clean")

        accessDF.printSchema()


//        topNVideoStat(spark, accessDF)

        cityTopNVideoStat(spark, accessDF)
        spark.stop()
    }

    def topNVideoStat(spark: SparkSession, accessDF: DataFrame){

//        //Use DataFrame API
//        import spark.implicits._
//
//        val df = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").
//          groupBy("day", "cmsID").
//          agg(count("cmsID").as("times")).orderBy($"times".desc)
//
//        df.show(false)

        //Use SQL

        try{
            accessDF.createOrReplaceTempView("log")
            val df = spark.sql("select day, cmsID, count(1) as times from log " +
              "where day='20170511' and cmsType='video' group by day, cmsID order by times desc")

            df.foreachPartition(partition_of_records => {

                val list = new ListBuffer[DayVideoAccess]

                partition_of_records.foreach(info => {
                    list.append(DayVideoAccess(
                        info.getAs[String]("day"),
                        info.getAs[Long]("cmsID"),
                        info.getAs[Long]("times"))
                    )
                })

                StatDAO.insertDayVideoAccessTopN(list)
            })
        } catch {
            case e:Exception => e.printStackTrace()
        }
    }

    def cityTopNVideoStat(spark: SparkSession, accessDF: DataFrame){

        try{

            import spark.implicits._
            var df = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").
              groupBy("day","city","cmsID").
              agg(count("cmsID").as("times"))

            df = df.select(df("day"), df("city"), df("cmsID"), df("times"),
                row_number().over(Window.partitionBy(df("city")).
                  orderBy(df("times").desc)).as("rank")).
              filter("rank <= 3")

            df.show()

            df.foreachPartition(partition_of_records => {

                val list = new ListBuffer[DayCityVideoAccess]

                partition_of_records.foreach(info => {
                    list.append(DayCityVideoAccess(
                        info.getAs[String]("day"),
                        info.getAs[Long]("cmsID"),
                        info.getAs[String]("city"),
                        info.getAs[Long]("times"),
                        info.getAs[Int]("rank"))
                    )
                })

                StatDAO.insertDayCVideoAccessTop3(list)
            })
        } catch {
            case e:Exception => e.printStackTrace()
        }

    }
}
