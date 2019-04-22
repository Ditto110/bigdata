//import org.apache.htrace.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
object StatKafkaStreaming {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("statKafkaStreaming").setMaster("local[2]");

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    val sc = sparkSession.sparkContext;
    val kafkaParams = Map(
    "bootStrap.servers" -> "192.168.52.40:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark_group"
    )
    val streamingContext = new StreamingContext(sparkConf,Seconds(10));
    val topic = Array("stat")
   /* val inputDStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )*/
  }
}
