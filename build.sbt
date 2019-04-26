name := "kafka-topic-mirror.scala"

organization := "co.cobli"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.2.0"
libraryDependencies += "org.scala-sbt" %% "command" % "1.2.8"
libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.4"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.26"

libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.5" % "test"

parallelExecution in Test := false

mainClass in (Compile, run) := Some("co.cobli.kafka.mirror.TopicsMirrorCommand")

mainClass in assembly := Some("co.cobli.kafka.mirror.TopicsMirrorCommand")
test in assembly := {}
assemblyJarName in assembly := s"kafka-topic-mirror.jar"
