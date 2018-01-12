package com.ssj.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object HiveContextApp {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    //测试和生产环境中，AppName和Master通过脚本进行指定
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    hiveContext.table("emp").show

    sc.stop()
  }
}

