package jp.gr.java_conf.massakai.kafka

import java.util.Collections

import kafka.api.{PartitionOffsetRequestInfo, FetchRequestBuilder}
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import collection.JavaConversions._

object KafkaConsumer {
  def findLeader(bootstraps: Seq[Broker], topic: String, partition: Int): Option[PartitionMetadata] = {
    for (bootstrap <- bootstraps) {
      val consumer = new SimpleConsumer(bootstrap.host, bootstrap.port, 100000, 64 * 1024, "leaderLookup")
      val topics = Collections.singletonList(topic)
      val req = new TopicMetadataRequest(topics)
      val resp = consumer.send(req)
      val metadata: java.util.List[TopicMetadata] = resp.topicsMetadata
      for (topicMetadata: TopicMetadata <- metadata) {
        for (partitionMetadata: PartitionMetadata <- topicMetadata.partitionsMetadata) {
          if (partitionMetadata.partitionId == partition) {
            return Some(partitionMetadata)
          }
        }
      }
    }
    None
  }
}

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
