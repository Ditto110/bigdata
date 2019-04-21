package com.fm.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  *         Created at 17:51.2019/4/21
  */
object StreamingReadFile {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("test")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    val lines = ssc.textFileStream("d:\\love.txt")
    val words = lines.flatMap(_.split(" "))
    val wordPair = words.map((_,1))
    val count = wordPair.reduceByKey(_+_)
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
