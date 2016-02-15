package jp.gr.java_conf.massakai.application

import jp.gr.java_conf.massakai.kafka.{Config, KafkaConsumer}
import scala.io.Source
import org.json4s.native.JsonMethods._

object HelloKafka extends App {
  // FIXME: 設定ファイルのパスは実行引数から取得する
  val configPath = "src/main/resources/config.json"
  val configSource = Source.fromFile(configPath)
  val configJson = parse(configSource mkString)

  implicit val formats = org.json4s.DefaultFormats
  val config = configJson.extract[Config]
  val broker = config.bootstrap.head
  // FIXME: パーティション毎にリーダーを選択する
  val clientName = "HelloKafka_" + broker.host + "_" + broker.port
  val consumer = new KafkaConsumer(
    broker.host,
    broker.port,
    config.consumer.soTimeout,
    config.consumer.bufferSize,
    clientName)

  val topic = config.topic.head
  val topicName = topic.name
  val partition = topic.partition.head
  val partitionId = partition.id
  var offset = partition.offset
  val response = consumer.getMessages(topicName, partitionId, offset, config.consumer.fetchSize)
  if (response.hasError) {
    // TODO: エラー処理を追加する
    println("Error")
  } else {
    val messageAndOffsetIterator = response.messageSet(topicName, partitionId).iterator()
    while (messageAndOffsetIterator.hasNext) {
      val messageAndOffset = messageAndOffsetIterator.next()
      val payload = messageAndOffset.message.payload
      val bytes = new Array[Byte](payload.limit)
      messageAndOffset.message.payload.get(bytes)
      val message = new String(bytes)
      println(messageAndOffset.offset + ": " + message)
      offset = messageAndOffset.nextOffset
    }
  }
}