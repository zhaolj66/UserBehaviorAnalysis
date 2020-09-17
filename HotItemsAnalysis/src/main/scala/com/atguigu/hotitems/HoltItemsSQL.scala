package com.atguigu.hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableConfig}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HoltItemsSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[UserBehavior] = env.readTextFile("C:\\Users\\zlj\\ideaproject\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(
        data => {
          val strings = data.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
        }
      ).assignAscendingTimestamps(_.timestamp * 1000L)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,settings)
   // val table: Table = tableEnv.from("inputStream")
    tableEnv.createTemporaryView("hottable",inputStream,'itemId,'behavior,'timestamp.rowtime as 'ts)
    val resultTable =tableEnv.sqlQuery(
      """
        |select
        |*
        |from
        |(
        |select
        |*,
        |row_number()
        |   over(partition by windowEnd order by cnt desc)
        |   as row_num
        |   from
        |   (
        |   select
        |     itemId,
        |     hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd,
        |     count(itemId) as cnt
        |     from hottable
        |     where behavior = 'pv'
        |     group by
        |     itemId,
        |     hop(ts,interval '5' minute,interval '1' hour)
        |   )
        |)
        |where row_num<=5
      """.stripMargin)
    resultTable.toRetractStream[Row].print()
    env.execute()
  }
}
