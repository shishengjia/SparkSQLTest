package com.ssj.spark

import org.apache.spark.sql.SparkSession

object DataFrameCase {
  def main(args: Array[String]){

    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/shishengjia/data/student.data")

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.show(30, false)

    studentDF.take(10)

    studentDF.first()

    studentDF.head(3)

    studentDF.filter("name='' OR name='NULL'").show()

    studentDF.select("email").show()

    studentDF.filter("SUBSTR(name,0,1)='M'").show()

    studentDF.select(studentDF.col("name").as("student_name")).show()

    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //default inner join
    studentDF.join(studentDF2, studentDF2.col("id") === studentDF.col("id")).show()

    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)
}
