package jp.gr.java_conf.massakai.kafka

import kafka.api.FetchRequestBuilder
import kafka.javaapi.FetchResponse
import kafka.javaapi.consumer.SimpleConsumer

case class KafkaConsumer(leadBroker: String, port: Int, soTimeout: Int, bufferSize: Int, clientName: String) {
  val consumer = new SimpleConsumer(leadBroker, port, soTimeout, bufferSize, clientName)


  def getMessages(topic: String, partition: Int, offset: Long, fetchSize: Int): FetchResponse = {
    val request = new FetchRequestBuilder()
      .clientId(clientName)
      .addFetch(topic, partition, offset, fetchSize)
      .build()
    consumer.fetch(request)
  }

  // TODO: 後始末する
  // consumer.close()
}
