package com.atguigu.hotitems

import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门商品topn
  */
case class UserBehavior(userId:Long,itemId:Long,categroy:Int,behavior :String,timestamp:Long)
//中间转换样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //读取
    val inputDstream: DataStream[String] = env.readTextFile("C:\\Users\\zlj\\ideaproject\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val middleDstream: DataStream[ItemViewCount] = inputDstream.map(
      data => {
        val strings = data.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemViewWindowResult())
    val resultDstream: DataStream[String] = middleDstream
      .keyBy("windowEnd") //按照窗口分组收集topn
      .process(new TopNProcess(10))
    //自定义处理六i成
    resultDstream.print()
    env.execute("HotItems")
  }
}
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator
//用在session
  override def merge(a: Long, b: Long) = a+b
}
class ItemViewWindowResult extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //接受aggregate数据
    val item_id: Long =key.asInstanceOf[Tuple1[Long]].f0
    val windowEd: Long = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(item_id,windowEd,count))

  }
}
class TopNProcess(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {
//liststate
  private var itemViewCountListState:ListState[ItemViewCount] =_
  override def open(parameters: Configuration) = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("ItemState",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]) = {
    itemViewCountListState.add(value)//每来一条数据加入
    //注册定时器，windowend+1的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
    val allItemViewCounts:ListBuffer[ItemViewCount] = ListBuffer()
    val iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      allItemViewCounts+=iter.next()
    }
    //清空状态
    itemViewCountListState.clear()
    val sortedResult: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 控制输出频率，模拟实时滚动结
    out.collect(sortedResult.toString)

  }
}