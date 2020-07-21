
package co.cobli.kafka.mirror

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.config.ConfigResource


class TopicsReaderClient(bootstrapServers: String) {

    protected val client: Admin = {
      val properties = new Properties
      properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      Admin.create(properties)
    }

    def getNonInternalTopics: Set[String] = {
      val allTopics = client.listTopics()
      allTopics.names().get().asScala.toSet
    }

    def getTopicsInfo(topics: Set[String] = getNonInternalTopics): Map[String, TopicInfo] = {
      val topicsParam = topics.map(topicConfigResource).asJavaCollection

      val topicsConfigs = client.describeConfigs(topicsParam).all.get

      val topicsDescriptions = client.describeTopics(topics.asJavaCollection).all.get.asScala

      topicsDescriptions.mapValues(
        topicDescription => {
          val configIndex = topicConfigResource(topicDescription.name)
          val config = topicsConfigs.get(configIndex).entries.asScala.map(
            entry => entry.name -> entry.value
          )
          TopicInfo(
            numPartitions = topicDescription.partitions.size,
            replicationFactor = topicDescription.partitions.iterator.next.replicas.size,
            config = config.toMap
          )
        }
      ).toMap
    }

    protected def topicConfigResource(name: String): ConfigResource ={
      new ConfigResource(ConfigResource.Type.TOPIC, name)
    }

    def close(): Unit ={
      client.close()
    }
}
