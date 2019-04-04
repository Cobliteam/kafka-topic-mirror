
package co.cobli.kafka.mirror

import scala.collection.JavaConverters._

import org.apache.kafka.clients.admin.Config


case class TopicInfo(numPartitions: Int, replicationFactor: Int, config: Map[String, String])

object TopicInfo {
  def apply(numPartitions: Int, replicationFactor: Int, config: Option[Config]): TopicInfo = {
    new TopicInfo(numPartitions: Int, replicationFactor, config.get.entries().asScala.map(entry => entry.name() -> entry.value()).toMap)
  }
}