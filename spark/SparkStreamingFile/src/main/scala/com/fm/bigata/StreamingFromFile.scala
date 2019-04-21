package com.fm.bigata

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  *         Created at 19:13.2019/4/21
  */
object StreamingFromFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming From File")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.textFileStream("d:\\love.txt")
    lines.cache()//持久化
    val count = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
