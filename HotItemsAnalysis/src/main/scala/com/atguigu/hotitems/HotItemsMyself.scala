package com.atguigu.hotitems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 整体思路就是先在同一窗口下聚合出count值，之后排序排序需要把同一窗口的数据按照count值排序，需要注册
  */
object HotItemsMyself {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.readTextFile("C:\\Users\\zlj\\ideaproject\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(
        data=>{
          val strings = data.split(",")
          UserBehavior(strings(0).toLong,strings(1).toLong,strings(2).toInt,strings(3),strings(4).toLong)
        }
      ).assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior=="pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new Myagg(),new Mywin())
        .keyBy("windowEnd")
        .process(new Myprocess(10))
      .print()
      env.execute("mysql")
  }
}
class  Myagg() extends AggregateFunction[UserBehavior,Long,Long] {
  override def add(value: UserBehavior, accumulator: Long) = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}
class Mywin() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    val itemid: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count = input.iterator.next()
    val windowEnd = window.getEnd
    out.collect(new ItemViewCount(itemid, windowEnd,count ))
  }
}
class  Myprocess(topSize:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {
  //定义一个保存状态的列表
  var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration) = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("ItemCount", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]) = {
    //先把所有的状态都放进去，然后触发定时器进行排序
    itemViewCountListState.add(value)
    //c触发定时器

      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)


  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
    //获得state
    var itemViewCountList: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      itemViewCountList += iter.next()
    }
    itemViewCountListState.clear()
    val counts = itemViewCountList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    out.collect(counts.toString())
  }
}