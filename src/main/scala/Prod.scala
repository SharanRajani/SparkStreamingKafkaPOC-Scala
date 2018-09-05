import java.util.{Calendar, Properties}

import com.exa.User
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Prod {
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    properties.setProperty("value.serializer", classOf[KafkaAvroSerializer].getName)
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")
    properties.put("auto.offset.reset", "latest")

    val topic = "kspark"
    val producer = new KafkaProducer[Integer, User](properties)
    val r = new Random()
    r.setSeed(Calendar.getInstance().get(Calendar.SECOND))
    var cnt = 0
    var key = 1
    try {
      while (true) {
        key=r.nextInt(10)
        val user = User.newBuilder().setField1(r.nextBoolean().toString).build()
        val pr = new ProducerRecord[Integer, User](topic, key, user)
        val metadata = producer.send(pr).get()
        println(metadata)
        Thread.sleep(r.nextInt(1500))
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer.flush()
    producer.close()
  }
}