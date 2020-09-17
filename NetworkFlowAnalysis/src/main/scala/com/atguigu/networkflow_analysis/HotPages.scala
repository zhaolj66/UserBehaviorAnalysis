package com.atguigu.networkflow_analysis

import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//定义样例类
case class ApacheLogEvent(ip:String,userId:String,timestap:Long,method:String,url:String)
//先聚合，在排序
case class PageViewCount(url:String,window:Long,count:Long)
object HotPages {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStateBackend(new FsStateBackend("hdfs"))
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[ApacheLogEvent] = env.readTextFile("C:\\Users\\zlj\\ideaproject\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(
        data => {
          val strings = data.split(" ")
          //对事件事件进行转换得到时间戳
          val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val ts = dateFormat.parse(strings(3)).getTime
          ApacheLogEvent(strings(0), strings(1), ts, strings(5), strings(6))
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(100)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.timestap
    })
    //开船聚合，排序输出
    inputStream
      .filter(_.method=="GET")
      .keyBy("url")
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageViewWindow())
      .keyBy("window")
      .process(new hpProcess(5))
      .print()
    env.execute()
  }
}
class PageCountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long] {
  override def add(value: ApacheLogEvent, accumulator: Long) = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}
class  PageViewWindow() extends WindowFunction[Long,PageViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    val url = key.asInstanceOf[Tuple1[String]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(PageViewCount(url,windowEnd,count))
  }
}
class hpProcess(topSize:Int) extends KeyedProcessFunction[Tuple,PageViewCount,String] {
var listState:ListState[PageViewCount]=_
  override def open(parameters: Configuration) = {
    listState=  getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("page",classOf[PageViewCount]))
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, PageViewCount, String]#OnTimerContext, out: Collector[String]) ={
    val listBuffer:ListBuffer[PageViewCount] = new ListBuffer()
    val iter: util.Iterator[PageViewCount] = listState.get().iterator()
    while(iter.hasNext){
      listBuffer+=iter.next()
    }
    //清空状态
    listState.clear()
    //排序输出
    val viewCounts: mutable.Seq[PageViewCount] = listBuffer.sortBy(-_.count).take(topSize)
    out.collect(viewCounts.toString())

  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Tuple, PageViewCount, String]#Context, out: Collector[String]) = {
    listState.add(value)
    ctx.timerService().registerEventTimeTimer(value.window+1)
    //此处的window收watermark的影响，windowEnd，可以把watermark理解为调满了窗口关闭时间，watermark是一种时间调慢的机制（只增不减）

  }
}