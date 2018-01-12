package com.ssj.spark

import java.sql.DriverManager


/**
  * use JDBC
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]){
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://10.211.55.9:14000", "root", "")
    val pstmt = conn.prepareStatement("select ename, sal from emp")
    val rs = pstmt.executeQuery()

    while (rs.next()){
      println("ename:" + rs.getString("ename") + " sal:" + rs.getDouble("sal"))
    }

    rs.close()
    pstmt.close()
    conn.close()

  }
}
