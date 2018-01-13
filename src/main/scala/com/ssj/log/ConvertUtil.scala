package com.ssj.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ConvertUtil {

    val struct = StructType(
        Array(
            StructField("url", StringType),
            StructField("cmsType", StringType),
            StructField("cmsID", LongType),
            StructField("traffic", LongType),
            StructField("ip", StringType),
            StructField("city", StringType),
            StructField("time", StringType),
            StructField("day", StringType)
        )
    )

    def parseLog(log:String) = {
        try{
            val splits = log.split("\t")

            val url = splits(1)
            val traffic = splits(2).toLong
            val ip = splits(3)

            val domain = "http://www.imooc.com/"

            val cms = url.substring(url.indexOf(domain) + domain.length)
            val tmp = cms.split("/")

            var cmsType = ""
            var cmsID = 0l

            if(tmp.length > 1){
                cmsType = tmp(0)
                cmsID = tmp(1).toLong
            }

            val city = ""
            val time = splits(0)
            val day = time.substring(0, 10).replaceAll("-", "")

            Row(url, cmsType, cmsID, traffic, ip, city, time, day)
        } catch {
            case e: Exception => Row(0)
        }

    }


}
