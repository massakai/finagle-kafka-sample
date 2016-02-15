package jp.gr.java_conf.massakai.kafka

case class Broker(host: String, port: Int)

case class Partition(id: Int, offset: Long)

case class Topic(name: String, partition: Seq[Partition])

case class Consumer(soTimeout: Int, bufferSize: Int, fetchSize: Int)

case class Config(bootstrap: Seq[Broker], topic: Seq[Topic], consumer: Consumer)
