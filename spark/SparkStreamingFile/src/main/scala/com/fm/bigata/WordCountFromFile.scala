package com.fm.bigata

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ASUS
  *         Created at 18:55.2019/4/21
  */
object WordCountFromFile {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[1]").setAppName("test");
    val sc=new SparkContext(conf);
    val textFile=sc.textFile(args(0));
    //词频统计
    val wc=textFile.flatMap(line =>line.split(" ")).map(x=>(x,1)).reduceByKey(_+_);
    //控制台输出结果
    wc.collect().foreach(println)
    wc.saveAsTextFile(args(1))
    sc.stop();
  }
}
