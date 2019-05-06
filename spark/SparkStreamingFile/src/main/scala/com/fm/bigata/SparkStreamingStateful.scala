package com.fm.bigata

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * @author ASUS
  *         Created at 19:13.2019/4/21
  *         通过checkPoint 实现有状态的算子转换
  *         （stateful transformations,例如：updateStateByKey/ReduceByKeyAndWindow/mapWithState）
  */
object SparkStreamingStateful {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming From File")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("hdfs://192.168.52.40:9000/sparkStreaming/chk")
    val lines = ssc.socketTextStream("192.168.52.40",9999,storageLevel = StorageLevel.MEMORY_AND_DISK)
    val pairs = lines.flatMap(_.split(" ")).map((_,1))
    pairs.checkpoint(Seconds(10))
    val logical = (word: String, count: Option[Int], state: State[Int]) => {
      if (state.isTimingOut()) {
        System.out.print(word + "is timeout")
      } else {
        val sum = count.getOrElse(0) + state.getOption().getOrElse(0)
        state.update(sum)
        val output = (word, sum)
      }
    }
    val result = pairs.mapWithState(StateSpec.function(logical))
//    result.print()
    result.stateSnapshots().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
