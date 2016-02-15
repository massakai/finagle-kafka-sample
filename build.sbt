name := "finagle-kafka-sample"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "com.twitter" %% "finagle-http" % "6.33.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.2"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"