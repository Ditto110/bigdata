import org.apache.spark.sql.SparkSession

object StatSpark_DF {
  def main(args: Array[String]): Unit = {
    val sparkSession =SparkSession.builder().appName("SparkTest2").master("local").getOrCreate();
    val input = sparkSession.read.json("C:\\Users\\sdt14325\\Desktop\\sparkSql_jsonArray.txt")
//    val input = sparkSession.read.json("C:\\Users\\sdt14325\\Desktop\\sparkSql.txt")
    input.show()




    val LCar = input.createOrReplaceTempView("LuxueryCars")
    sparkSession.sql("select * from LuxueryCars where weight > 1000").show()
//    input.withColumn("weight2",input("weight")).show()

    println("union")
    //    input.union(input.limit(1)).show()
    //    input.unionByName(input.limit(2)).show()
    //    input.join(input2)
    //    input.join(input2,"name")
    //    input.join(input2,Seq("name","weight"),"inner")
    //    input.stat.freqItems(Seq("weight"),0.3).show()
    //    input.intersect(input2.limit(2)).show()
    //    input.except(input2.limit(2)).show()
    //    input.withColumnRenamed("weight2","weight").show()

//    input.filter("weight = 1800").show()
//    input.where("weight = 1490 or weight = 1800").show()
//    input.where("weight = 1800").groupBy("speed").count().show()
//    input.groupBy("weight").max().show()
//    input.agg("speed" -> "max" ,"weight" ->"sum").show()
//    println("agg=========================")
//    input.groupBy("weight").agg("speed" -> "max").show()
//    println("groupby agg ==============")

//    println("第一条记录")
//    val firstRec = input.first()
//    println(firstRec)
//    val header12 = input.head(2)
//    println("前两条记录")
//    println(header12)
//    val inputToArray =  input.collect()
//    println("output")
//    print(inputToArray)
//    val inputToList = input.collectAsList()
//    println("outPutList")
//    println(inputToList)
//    println("排序")
//    input.orderBy(input("weight").desc).show()
//    input.sort("weight").show()
//    input.sort(input("weight").desc).show()
  }

  def tFan[T>:App](v:T):Unit = {
    println(v)
  }

  def tFlag[T<:Appendable]():Int = {
    println("asd")
    2
  }
}
