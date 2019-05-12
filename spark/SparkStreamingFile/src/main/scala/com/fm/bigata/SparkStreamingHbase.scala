package com.fm.bigata

import org.apache.hadoop.fs.DF
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ASUS
  * Created at 23:59.2019/4/22
  */
class SparkStreamingHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Sql basic example")
      .master("")
      .getOrCreate()
    //配置参数
    import spark.implicits._
    val kafkaParams = Map("bootstrap.servers" -> "192.168.52.40:9092,192.168.52.35:9092,192.168.52.41:9092", //brokers
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_group")
    val topic = Array("stat") //主题
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( //读取数据流
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams))
    var rank = 0; //用来记录当前数据序号
    val lines = kafkaStream.window(Seconds(10), Seconds(3)).flatMap(line => {
      Some(line.toString)
    }).foreachRDD({ rdd: RDD[String] =>
      val df = rdd.flatMap(_.split(" ")).toDF.withColumnRenamed("_1", "word")
      val table = df.createOrReplaceTempView("words")
      val ans = spark.sql("select word, count(*) as total from words group by word order by count(*) desc").limit(5).map(x => {
        rank += 1
        (rank, x.getString(0), x.getLong(1))
      })
    }
    )
  }
}
