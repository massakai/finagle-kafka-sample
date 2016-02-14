package jp.gr.java_conf.massakai.application

import kafka.api.FetchRequestBuilder
import kafka.javaapi.consumer.SimpleConsumer;

object HelloKafka extends App {
  val leadBroker = "localhost"
  val port = 9093
  val clientName = "HelloKafka"
  val consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName)

  val topic: String = "test"
  val partition = 0
  var offset: Long = 0
  val request = new FetchRequestBuilder()
    .clientId(clientName)
    .addFetch(topic, partition, offset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
    .build()

  val response = consumer.fetch(request)
  if (response.hasError) {

  } else {
    val messageAndOffsetIterator = response.messageSet(topic, partition).iterator()
    while (messageAndOffsetIterator.hasNext) {
      val messageAndOffset = messageAndOffsetIterator.next()
      val payload = messageAndOffset.message.payload
      val message = new Array[Byte](payload.limit)
      messageAndOffset.message.payload.get(message)
      println(messageAndOffset.offset + ": " + message)
      offset = messageAndOffset.nextOffset
    }
  }

  consumer.close()
}