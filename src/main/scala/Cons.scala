import com.exa.User
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}


object Cons {
  def main(args: Array[String]) {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9092",
      "key.deserializer" -> classOf[IntegerDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "schema.registry.url" -> "http://127.0.0.1:8081",
      "group.id" -> "customer-group1",
      "specific.avro.reader" -> "true",
      "auto.offset.reset" -> "latest"
    )

    val topics = List("kspark")
    val sparkConf = new SparkConf().setAppName("KSS").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val stream = KafkaUtils.createDirectStream[Integer, User](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[Integer, User](topics, kafkaParams)
    )
    var newstream = stream.map(record => (record.value.getField1))
    var beg=newstream.map(vl=>(vl,1))
    var wordcount = beg.reduceByKey((x, y) => x + y)
    wordcount.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}