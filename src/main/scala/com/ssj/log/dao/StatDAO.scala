package com.ssj.log.dao

import java.sql.{Connection, PreparedStatement}

import com.ssj.log.model.{DayCityVideoAccess, DayVideoAccess}
import com.ssj.log.utils.MySQLUtil

import scala.collection.mutable.ListBuffer

object StatDAO {

    def insertDayVideoAccessTopN(list:ListBuffer[DayVideoAccess]) = {
        var connection:Connection = null
        var pstmt:PreparedStatement = null

        try{

            connection = MySQLUtil.getConnection()
            connection.setAutoCommit(false)

            val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values(?,?,?)"
            pstmt = connection.prepareStatement(sql)

            for(ele <- list){
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsID)
                pstmt.setLong(3, ele.times)
                pstmt.addBatch()
            }

            // more efficient using batch
            pstmt.executeBatch()
            connection.commit()

        } catch {
            case e:Exception => e.printStackTrace()
        } finally {
            MySQLUtil.closeConnection(connection, pstmt)
        }
    }

    def insertDayCVideoAccessTop3(list:ListBuffer[DayCityVideoAccess]) = {
        var connection:Connection = null
        var pstmt:PreparedStatement = null

        try{

            connection = MySQLUtil.getConnection()
            connection.setAutoCommit(false)

            val sql = "insert into day_video_city_access_top3_stat(day,cms_id,city,times,times_rank) values(?,?,?,?,?)"
            pstmt = connection.prepareStatement(sql)

            for(ele <- list){
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsID)
                pstmt.setString(3, ele.city)
                pstmt.setLong(4, ele.times)
                pstmt.setInt(5, ele.rank)
                pstmt.addBatch()
            }

            // more efficient using batch
            pstmt.executeBatch()
            connection.commit()

        } catch {
            case e:Exception => e.printStackTrace()
        } finally {
            MySQLUtil.closeConnection(connection, pstmt)
        }

    }


}


