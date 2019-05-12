package com.fm.bigata

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  *         Created at 15:56.2019/5/4
  */
object SparkStreamingFromNc4 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("sparkStreaming from nc").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(5))
    val lines = ssc.socketTextStream("192.168.128.132", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD(rdd =>{
      ssc.checkpoint("hdfs://192.168.128.132:8020//checkpoint")
      import sparkSession.implicits._
      val wordCountDF  = rdd.toDF("word")
      wordCountDF.createOrReplaceTempView("words")
      val result  = sparkSession.sql("select word,count(1) from words group by word")
      result.show()
      Thread.sleep(1000)
    })

   /* val pairs = words.map((_,1))
    pairs.updateStateByKey((c:Seq[Int],p:Option[Int]) =>{
      val sum = c.sum
      val a = p.getOrElse(0)
      Some(sum+a)
    })*/
/*    val  addFun = (c:Seq[Int],p:Option[Int]) =>{
      val sum = c.sum
      val i = p.getOrElse(0)
      Some(sum+i)
    }
    pairs.updateStateByKey(addFun)*/
    ssc.start()
    ssc.awaitTermination()

  }
}
