import org.apache.spark.sql.SparkSession
object StatSpark_RDD {
  def main(args: Array[String]): Unit = {
    val sparkSqlContext = SparkSession.builder().appName("sparkTest").master("local").getOrCreate()
    val df = sparkSqlContext.read.json("C:\\Users\\sdt14325\\Desktop\\sparkSql.txt")
    val people = df.createOrReplaceTempView("people")
    sparkSqlContext.sql("select * from people where pv > 10").show()
    println("people")
  }
}
