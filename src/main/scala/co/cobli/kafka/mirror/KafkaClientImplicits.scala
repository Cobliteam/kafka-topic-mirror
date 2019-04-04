
package co.cobli.kafka.mirror

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import kafka.server.ConfigType
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AdminClient, Config, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.Topic


/**
  * Complements KafkaClient's (KafkaZkClient and AdminClient) with standardized methods for topic management
  *
  */
object KafkaClientImplicits {

  implicit class TopicsReadableZkAdapter(client: KafkaZkClient) extends TopicsReading {

    def getNonInternalTopics: Set[String] = {
      val allTopics = client.getAllTopicsInCluster.toSet
      allTopics.filterNot(Topic.isInternal)
    }

    def getTopicsInfo(topics: Set[String] = getNonInternalTopics): Map[String, TopicInfo] = {
      val adminZkClient = new AdminZkClient(client)

      val topicsInfo: mutable.Map[String, TopicInfo] = mutable.Map.empty

      val assignments = client.getPartitionAssignmentForTopics(topics)

      for (topic <- topics) {
        if (!client.isTopicMarkedForDeletion(topic)) {
          val topicPartitionAssignment = assignments(topic)
          val replicationFactor = topicPartitionAssignment.head._2.size
          val numPartitions = topicPartitionAssignment.size
          val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic).asScala
          topicsInfo += (topic -> TopicInfo(
            numPartitions = numPartitions,
            replicationFactor = replicationFactor,
            config = configs.toMap
          ))
        }
      }
      topicsInfo.toMap
    }
  }

  implicit class TopicsManageableKafkaAdapter(client: AdminClient) extends TopicsManaging {

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

    def createTopic(newTopic: NewTopic): Unit = {
      client.createTopics(Set(newTopic).asJava).all.get
    }

    def alterTopicConfig(name: String, config: Map[String, String]): Unit = {
      val newConfig = new Config(
        config.map { case (key, value) => new ConfigEntry(key, value) }.asJavaCollection
      )

      val alterParam = new util.HashMap[ConfigResource, Config] {
        put(topicConfigResource(name), newConfig)
      }

      client.alterConfigs(alterParam).all.get
    }

    private def topicConfigResource(name: String): ConfigResource ={
      new ConfigResource(ConfigResource.Type.TOPIC, name)
    }
  }

}