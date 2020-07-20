import com.typesafe.sbt.packager.docker.DockerPermissionStrategy.CopyChown


name := "kafka-topic-mirror"
maintainer := "ivan.stoiev@cobli.co"
organization := "co.cobli"

// Let version come from CI branches/tags
version := {
  if(insideCI.value) {
    if (sys.env.contains("CIRCLE_TAG")){
      sys.env("CIRCLE_TAG")
    } else if(sys.env("CIRCLE_BRANCH") == "master"){
      "latest"
    } else {
      sys.env("CIRCLE_BRANCH")
    }
  } else {
    version.value
  }
}


scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.2.0"
libraryDependencies += "org.scala-sbt" %% "command" % "1.2.8"
libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.4"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.26"

mainClass in (Compile, run) := Some("co.cobli.kafka.mirror.TopicsMirrorCommand")

// Docker building
enablePlugins(JavaAppPackaging, DockerPlugin)
daemonUser in Docker := "kafka-topic-mirror"
dockerBaseImage := "openjdk:11.0.7-jre-slim-buster"
// No cleaning on CI
dockerAutoremoveMultiStageIntermediateImages := {
  if (insideCI.value)
    false
  else
    true
}

// Docker publishing
dockerUsername := Some("cobli")
