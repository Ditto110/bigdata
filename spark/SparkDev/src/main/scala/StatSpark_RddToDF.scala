import org.apache.spark.sql.SparkSession

/**
  * RDD与DF之间的操作
  */
object StatSpark_RddToDF {

  case class  Person(name:String,age:String)

  def main(args: Array[String]): Unit = {
    val sparkSession  = SparkSession.builder().master("local").appName("StatSpark_RddToDf").getOrCreate()
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile("C:\\Users\\sdt14325\\Desktop\\sparkSql 2.txt")
//        rdd.flatMap(_.split(" ")).map((_,1)).foreach(pair => println(pair._1 + ":" + pair._2))
    //将RDD转换成DF
    val person2 = rdd.map(_.split(",")).map(p =>Person(p(0), p(1))).toDF()
    person2.show()
  }
}
