package com.fm.bigata

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  *         Created at 15:56.2019/5/4
  */
object SparkStreamingFromNc {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("sparkStreaming from nc").getOrCreate()
    import sparkSession.implicits._
    val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(10))
    val lines = ssc.socketTextStream("192.168.128.132", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
//    val wordCount = words.map((_,1)).reduceByKey((_+_))
    //每10 秒对过去30s 秒的数据重新整理一次
    val wordCount = words.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))
    println("============")
    wordCount.print()
    println("============")
    ssc.start()
    ssc.awaitTermination()

  }
}
