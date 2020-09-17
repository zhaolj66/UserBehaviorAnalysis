package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputDstream = env.readTextFile(resource.getPath)
    val middleDstream= inputDstream.map(//转换为样例类并提取
      data => {
        val strings = data.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)
    val uvStream =
      middleDstream
//        .filter(_.behavior=="pv")
//        .map(data =>("uv",data.userId))
//        .keyBy(_._1)
//        .timeWindow(Time.hours(1))
//          .trigger()
//        .process()

  }
}
//自定义布隆过滤器,z主要就是位图和hash函数
class Bloom(size:Long) extends Serializable{
  private  val cap = size
  //hash函数
  def hash(value:String,seed:Int):Long={
    var result = 0
    for(i<-0 until value.length){
      result = result* seed+value.charAt(i)
    }
    //返回hash值，映射到cap范围
    (cap -1)& result //得到
  }
}