package com.ssj.spark

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

    //SimpleDateFormat is not thread safe, so use FastDateFormat
    val original_format = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

    val target_format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    def parse(time:String) = {
        target_format.format(new Date(get_time(time)))
    }


    /**
      *
      * @param time: [10/Nov/2016:00:01:02 +0800]
      */
    def get_time(time:String) = {
        try{
            original_format.parse(time.substring(time.indexOf("[")+1, time.indexOf("]"))).getTime
        } catch {
            case e: Exception => {
                0l
            }
        }
    }

    def main(args: Array[String]){
        println(parse("[10/Nov/2016:00:01:02 +0800]"))
    }

}
