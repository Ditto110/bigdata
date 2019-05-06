package com.fm.bigata

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * @author ASUS
  *         Created at 19:13.2019/4/21
  *         通过checkPoint实现driver状态恢复
  */
object SparkStreamingStateful2 {

  def main(args: Array[String]): Unit = {

    val chkpoint = "hdfs://192.168.52.40:9000/sparkStreaming/chk"
    val ssc = StreamingContext.getOrCreate(chkpoint,()=>createSSC(chkpoint))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.start()
    ssc.awaitTermination()
  }

  def createSSC(chkPoint:String):StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming From File")
    val ssc = new StreamingContext(conf,Seconds(10))
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
    ssc.checkpoint(chkPoint)
    ssc
  }
}
