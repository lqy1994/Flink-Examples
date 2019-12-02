package com.meituan.meishi.data.lqy.flink.proj.model

//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long,
                        categoryId: Int, behavior: String, timestamp: Long) {

}

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long) {

}
