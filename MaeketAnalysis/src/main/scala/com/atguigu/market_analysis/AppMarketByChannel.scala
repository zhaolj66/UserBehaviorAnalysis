package com.atguigu.market_analysis

import java.util.{Random, UUID}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class MarketUserBehavior(userId:String,behavior:String,channle:String,timestamp:Long)
//自定义数据源
class SimlatedSource()extends RichSourceFunction[MarketUserBehavior] {
  var running = true
  val rand:Random = new  Random()

  //定义用户和行为的渠道的集合
  var behaviorSet :Seq[String] = Seq("view" ,"dowload","install","uninstall")
  val channelSet:Seq[String] =Seq("appstore","weibo","wechat","tieba")
  override def cancel() = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]) = {
    val maxCounts = Long.MaxValue
    var count = 0L
    while(running&&count<maxCounts){
      val id =UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(id,behavior,channel,ts))
      count+=1
      Thread.sleep(50L)
    }
  }
}
object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.addSource(new SimlatedSource)
      .assignAscendingTimestamps(_.timestamp)

    dataStream
      .map(data=>(data.channle+"_"+data.behavior,1))
      .keyBy(_._1)
      .timeWindow(Time.minutes(10),Time.seconds(10))
      .aggregate(new Mychannl(),new MychannlWindow())
      .print()
    env.execute()
  }
}
class Mychannl() extends AggregateFunction[(String,Int),Long,Long] {
  override def add(value: (String, Int), accumulator: Long) = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}
class MychannlWindow extends WindowFunction[Long,(String,Long,Long),String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long,Long)]): Unit = {
    out.collect((key,input.iterator.next(),window.getEnd))
  }
}