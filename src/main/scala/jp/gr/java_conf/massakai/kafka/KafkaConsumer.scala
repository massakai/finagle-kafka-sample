package jp.gr.java_conf.massakai.kafka

import java.util

import kafka.api.{PartitionOffsetRequestInfo, FetchRequestBuilder}
import kafka.common.TopicAndPartition
import kafka.javaapi.{OffsetRequest, FetchResponse}
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

  def getLastOffset(topic: String, partition: Int, whichTime: Long): Long = {
    val topicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
      0L
    } else {
      response.offsets(topic, partition)(0)
    }
  }

  // TODO: 後始末する
  // consumer.close()
}
