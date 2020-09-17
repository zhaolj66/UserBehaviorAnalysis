package com.atguigu.networkflow_analysis
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
case class UvCount(windowEnd:Long,count:Long)
object UniqueView {
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
       middleDstream.filter(_.behavior=="pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountResult())
      .print()
    env.execute()
  }
}
//自定义实现全串口
/**
  * 二期布隆过滤器去重
  */
class UvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
   var userIdSet = Set[Long]()
    val iter = input.iterator
    while (iter.hasNext){
        userIdSet+=iter.next().userId
    }
    //
    out.collect(UvCount(window.getEnd,userIdSet.size))
  }
}