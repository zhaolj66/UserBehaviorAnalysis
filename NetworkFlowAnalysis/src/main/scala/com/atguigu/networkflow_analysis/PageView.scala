package com.atguigu.networkflow_analysis
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class UserBehavior(userId:Long,itemId:Long,categroy:Int,behavior :String,timestamp:Long)
//定义输出样例类
case class PVCount(windowEnd:Long,count:Long)
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(6)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputDstream = env.readTextFile(resource.getPath)
    val middleDstream= inputDstream.map(//转换为样例类并提取
      data => {
        val strings = data.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)
    val pvStream = middleDstream
      .filter(_.behavior =="pv")
      .map(data=>(Random.nextString(10), 1)) //定义一个pv字符串作为dummy key,数据倾斜
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //一小时滚动
      .aggregate(new Pvagg(),new PvCountWindowResult())
   // pvStream.print("1")
      val totalStream =
        pvStream
          .keyBy(_.windowEnd)
          .process(new MyprocessCount())
.print()

    env.execute()
      }
}
class Pvagg() extends AggregateFunction[(String,Int),Long,Long] {
  override def add(value:(String,Int), accumulator: Long) = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}
class  PvCountWindowResult() extends WindowFunction[Long,PVCount,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PVCount]): Unit = {
    out.collect(PVCount(window.getEnd,input.head))
  }
}
class  MyprocessCount() extends KeyedProcessFunction[Long,PVCount,PVCount]{
var valueState:ValueState[Long] = _
  override def open(parameters: Configuration) = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value",classOf[Long]))
  }


  override def processElement(value: PVCount, ctx: KeyedProcessFunction[Long, PVCount, PVCount]#Context, out: Collector[PVCount]) = {
    valueState.update(valueState.value()+value.count)

    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)


  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PVCount, PVCount]#OnTimerContext, out: Collector[PVCount]) = {
    out.collect(PVCount(timestamp-1,valueState.value()))
    valueState.clear()
  }

}