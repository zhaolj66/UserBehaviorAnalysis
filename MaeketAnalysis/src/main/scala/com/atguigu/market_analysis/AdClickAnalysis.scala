package com.atguigu.market_analysis

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

//定义输入输出样例类
case class AdClickLog(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
case class AdCLickCountByProvince(windowEnd:Long,province:String,count:Long)
//测属出流样例类
case class BlankListUserWaring(userId:Long,adId:Long,msg:String)
object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val  resource = getClass.getResource("/AdClickLog.csv")
    val inputStream =
      env.readTextFile(resource.getPath)
      .map(
        data=>{
          val strings = data.split(",")

          AdClickLog(strings(0).toLong,strings(1).toLong,strings(2),strings(3),strings(4).toLong)
        }
      ).assignAscendingTimestamps(_.timestamp*1000L)

    //黑名单过滤
   val filterBlank:DataStream[AdClickLog] =  inputStream
      .keyBy(data=>(data.userId,data.userId))
      .process(new FilterBlankListAd(100))

    //
    filterBlank.getSideOutput(new OutputTag[BlankListUserWaring]("warning")).print("warning")

    filterBlank
      .keyBy(_.province)
      .timeWindow(Time.minutes(10))
      .aggregate(new AdCountAgg(),new AdCountWindow())
      .print("finall")
      env.execute()
  }
}
class AdCountAgg() extends AggregateFunction[AdClickLog,Long,Long] {
  override def add(value: AdClickLog, accumulator: Long) = accumulator+1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a+b
}
class  AdCountWindow() extends WindowFunction[Long,AdCLickCountByProvince,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCLickCountByProvince]): Unit = {
    out.collect(AdCLickCountByProvince(window.getEnd,key,input.iterator.next()))
  }
}
class FilterBlankListAd(maxCount:Int) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog] {
  //定义状态，保存点击量
var countState:ValueState[Long] = _
//定义每天0点清空状态的时间戳
var resetTImerState:ValueState[Long] = _
  //定以Valuestate表明是否已经加入黑名单
  var isBlankTImerState:ValueState[Boolean] = _
  override def open(parameters: Configuration) = {
    countState=  getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
    resetTImerState=  getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset",classOf[Long]))
    isBlankTImerState=  getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("boolean",classOf[Boolean]))

  }

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //判断只要是第一个数据来了，直接注册0点清空状态
    val curCount: Long = countState.value()
    if(curCount==0){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24)+1)*(1000*60*60*24)-8*60*60*1000
      resetTImerState.update(ts)
      ctx.timerService().registerEventTimeTimer(ts)
    }
if(curCount >= maxCount){
  //组织输出一次
  if(!isBlankTImerState.value()){
    isBlankTImerState.update(true)
    ctx.output(new OutputTag[BlankListUserWaring]("warning"),BlankListUserWaring(value.userId,value.adId,"warning"))
  }
  return
}
    //正常情况原样输出
    countState.update(curCount+1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]) = {
    if(timestamp==resetTImerState.value()){
      isBlankTImerState.clear()
      countState.clear()
    }
  }
}