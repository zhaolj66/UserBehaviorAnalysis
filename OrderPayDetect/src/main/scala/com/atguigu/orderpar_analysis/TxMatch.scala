package com.atguigu.orderpar_analysis


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

//定义到账事件样例类
case class  ReciptEvent(txId:String,payChannle:String,timestamp: Long)

object TxMatch {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //0.从文件读取数据
      val resource = getClass.getResource("/OrderLog.csv")
      val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
        .map(
          data => {
            val strings = data.split(",")
            OrderEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
          }

        ).assignAscendingTimestamps(_.timestamp * 1000L)
        .filter(_.eventType =="pay")
        .keyBy(_.txId)
      //2,到账流
      val resource2 = getClass.getResource("/ReceiptLog.csv")
      val receiptStream = env.readTextFile(resource2.getPath)
        .map(
          data => {
            val strings = data.split(",")
            ReciptEvent(strings(0), strings(1), strings(2).toLong)
          }

        ).assignAscendingTimestamps(_.timestamp * 1000L)
        .keyBy(_.txId)
      //合并两条流进行操作
      val resultStream =orderEventStream.connect(receiptStream)
            .process(new TxMatchResult())
    }
  }
}
class TxMatchResult() extends CoProcessFunction[OrderEvent,ReciptEvent,(OrderEvent,ReciptEvent)] {
  //定以状态，
  lazy val payEventState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order",classOf[OrderEvent]))
  lazy val reciptEventState:ValueState[ReciptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReciptEvent]("order",classOf[ReciptEvent]))
  //侧输出流标签
  val unmatchedPay = new OutputTag[OrderEvent]("unmatch-pay")
  val unmatchedRecipt = new OutputTag[ReciptEvent]("unmatch-recipt")
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReciptEvent, (OrderEvent, ReciptEvent)]#Context, out: Collector[(OrderEvent, ReciptEvent)]) = {
        //订单支付事件来了
    val recipt = reciptEventState.value()
    if(recipt != null){
      out.collect(pay,recipt)
      reciptEventState.clear()
      payEventState.clear()

    }else {
      //等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp*1000L+15000)
      payEventState.update(pay)
    }

  }

  override def processElement2(recipt: ReciptEvent, ctx: CoProcessFunction[OrderEvent, ReciptEvent, (OrderEvent, ReciptEvent)]#Context, out: Collector[(OrderEvent, ReciptEvent)]) = {
    val pay = payEventState.value()
    if(pay != null){
      out.collect(pay,recipt)
      reciptEventState.clear()
      payEventState.clear()

    }else {
      //等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp*1000L+15000)
      reciptEventState.update(recipt)
    }

  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReciptEvent, (OrderEvent, ReciptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReciptEvent)]) = {
    if(payEventState.value() != null){
      ctx.output(unmatchedPay,payEventState.value())

    }else if(reciptEventState.value() !=null){
      ctx.output(unmatchedRecipt,payEventState.value())
      reciptEventState.clear()
      payEventState.clear()
    }
  }
}