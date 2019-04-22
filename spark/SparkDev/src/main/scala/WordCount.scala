import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    args.foreach(println(_))
    val conf =  new SparkConf()
    conf.setAppName("test RDD")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    if(args.length > 0){
      println("result:")
      sc.textFile(args(0))
        .flatMap(line => line.split(" "))
        .map(word => (word,1))
        .reduceByKey(_+_)
        .foreach(pair => println(pair._1 + ":" + pair._2))
    }
    sc.stop()
  }
}
