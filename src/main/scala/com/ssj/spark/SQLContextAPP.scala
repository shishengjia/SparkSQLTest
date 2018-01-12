package com.ssj.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLContextAPP {
  def main(args: Array[String]){

    val path = args(0)

    val sparkConf = new SparkConf()

    //测试和生产环境中，AppName和Master通过脚本进行指定
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop()
  }
}
