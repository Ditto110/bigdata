package com.fm.bigata

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * @author SDT14325
  *         created at 9:15 2019/5/6 
  *
  */
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[2]").appName("sparkStreamingTest").getOrCreate()
    val ssc = new StreamingContext(session.sparkContext, Seconds(10))
    ssc.checkpoint("")
    val dStream = ssc.socketTextStream("192.168.52.40", 9999, StorageLevel.MEMORY_AND_DISK)
    val lines = dStream.flatMap(_.split(","))
    val pairs = lines.map((_, 1))
    //累计算法
    val logical = (word: String, count: Option[Int], state: State[Int]) => {
      if (state.isTimingOut()) {
        System.out.print(word + "is timeout")
      } else {
        val sum = count.getOrElse(0) + state.getOption().getOrElse(0)
        state.update(sum)
        val output = (word, sum)
      }
    }

    //通过mapWithState 实现累计计算
    val result = pairs.mapWithState(StateSpec.function(logical).timeout(Seconds(60)))
    //输出
    result.stateSnapshots().foreachRDD((rdd, time) => {
      println("rddCount" + rdd.count())
      rdd.foreach(x => println(x._1 + "=" + x._2))
    })
  }
}
