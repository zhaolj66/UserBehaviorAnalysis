package com.atguigu.orderpar_analysis

import java.sql.Timestamp
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//import javax.swing.event.HyperlinkEvent.EventType

case class OrderEvent(orderId:Long,eventType: String,txId:String,timestamp:Long)
case class  OrderResult(orderId:Long,resultMG:String)
object OrderTimeOut {
  def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //0.从文件读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEvent = env.readTextFile(resource.getPath)
      .map(
        data=>{
          val strings = data.split(",")
          OrderEvent(strings(0).toLong,strings(1),strings(2),strings(3).toLong)
        }

      ).assignAscendingTimestamps(_.timestamp*1000L)
      .keyBy(_.orderId)
    //1，定义pattern
    val orderPayPattern =
      Pattern.begin[OrderEvent]("create").where(_.eventType =="create")
      .followedBy("pay").where(_.eventType =="pay")
      .within(Time.minutes(15))
    //2应用到数据流上
    val patternStream = CEP.pattern(orderEvent,orderPayPattern)

    // 测输出流提取，用于处理超时事件
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeOut")
  // 4,调用select方法，匹配成功支付以及超时事件
    val resultStream = patternStream.select(orderTimeOutputTag
      ,new OrderTimeOutSelect()
      ,new OrderPaySelect())
      resultStream.print("secceed pay")
      resultStream.getSideOutput(orderTimeOutputTag).print()
    env.execute("order pay time")
  }
}
//自定以实现
class OrderTimeOutSelect() extends  PatternTimeoutFunction[OrderEvent,OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long):OrderResult = {
    val timeOutId = map.get("create").iterator().next().orderId
    OrderResult(timeOutId,l+"timeOut")
  }
}
class OrderPaySelect extends  PatternSelectFunction[OrderEvent,OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]) = {
    val Id = map.get("pay").iterator().next().orderId
    OrderResult(Id, "succeed")
  }
}