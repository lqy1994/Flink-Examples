package com.meituan.meishi.data.lqy.flink.proj.model

object FlowModels {

}

case class ApacheLogEvent(ip : String, userId: String, eventTime: Long, method: String, url: String) {

}

case class UrlViewCount(url: String, windowEnd: Long, count: Long) {

}
