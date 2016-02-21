package jp.gr.java_conf.massakai.application

import jp.gr.java_conf.massakai.kafka.{Partition, Config, KafkaConsumer}
import kafka.javaapi.PartitionMetadata
import scala.io.Source
import org.json4s.native.JsonMethods._
import kafka.common.ErrorMapping

object HelloKafka extends App {
  // FIXME: 設定ファイルのパスは実行引数から取得する
  val configPath = "src/main/resources/config.json"
  val configSource = Source.fromFile(configPath)
  val configJson = parse(configSource mkString)

  implicit val formats = org.json4s.DefaultFormats
  val config: Config = configJson.extract[Config]

  // TODO: 複数のトピックに対応する
  val topic = config.topic.head
  val topicName = topic.name
  // TODO: 複数のパーティションに対応する
  val partition: Partition = topic.partition.head
  val partitionId = partition.id

  // TODO: パーティション毎にリーダーを選択する
  val partitionMetadata: PartitionMetadata = KafkaConsumer.findLeader(config.bootstrap, topicName, partitionId).get
  val leaderBroker = partitionMetadata.leader
  val clientName = "HelloKafka_" + leaderBroker.host + "_" + leaderBroker.port
  val consumer = new KafkaConsumer(
    leaderBroker.host,
    leaderBroker.port,
    config.consumer.soTimeout,
    config.consumer.bufferSize,
    clientName)

  var readOffset = consumer.getLastOffset(topicName, partitionId, System.currentTimeMillis())
  val response = consumer.getMessages(topicName, partitionId, readOffset, config.consumer.fetchSize)
  if (response.hasError) {
    // TODO: エラー処理を追加する
    response.errorCode(topic.name, partitionId) match {
      case ErrorMapping.OffsetOutOfRangeCode => println("Error: Offset out of range")
      case _ => println("Error")
    }
  } else {
    val messageAndOffsetIterator = response.messageSet(topicName, partitionId).iterator()
    while (messageAndOffsetIterator.hasNext) {
      val messageAndOffset = messageAndOffsetIterator.next()
      val currentOffset = messageAndOffset.offset
      if (currentOffset < readOffset) {
        println("Found an old offset: " + currentOffset + " Expecting: " + readOffset)
      } else {
        val payload = messageAndOffset.message.payload
        val bytes = new Array[Byte](payload.limit)
        payload.get(bytes)
        val message = new String(bytes)
        println(currentOffset + ": " + message)
        readOffset = messageAndOffset.nextOffset
      }
    }
  }
}