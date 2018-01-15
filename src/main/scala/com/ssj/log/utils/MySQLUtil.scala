package com.ssj.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtil {

    def getConnection() = {
        DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project?user=root&password=Assjusher123")
    }

    def closeConnection(connection:Connection, pstmt:PreparedStatement) = {
        try{
            if(pstmt != null)
                pstmt.close()
        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            if(connection != null)
                connection.close()
        }
    }

}
