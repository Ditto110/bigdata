package com.fm.bigata

import javax.sql.ConnectionPoolDataSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  *         Created at 15:56.2019/5/4
  */
object SparkStreamingFromNc2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("sparkStreaming from nc").getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("WARN")
    val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(5))
    ssc.checkpoint("hdfs://192.168.128.132:8020//checkpoint")
    val lines = ssc.socketTextStream("192.168.128.132", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    // 函数字面量传值
    pairs.updateStateByKey((c:Seq[Int],p:Option[Int]) =>{
      val sum = c.sum
      val a = p.getOrElse(0)
      Some(sum+a)
    })
/*    val  addFun = (c:Seq[Int],p:Option[Int]) =>{
      val sum = c.sum
      val i = p.getOrElse(0)
      Some(sum+i)
    }
    pairs.updateStateByKey(addFun)*/
    pairs.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
