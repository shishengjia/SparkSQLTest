package com.ssj.log.utils

import com.ggstar.util.ip.IpHelper

/**
  * transform the ip address to corresponding city name
  * */

object IPUtil {

    def getCity(ip:String) = {
        IpHelper.findRegionByIp(ip)
    }

    def main(args: Array[String]){
        println(getCity("175.159.24.216"))
    }

}
