
package co.cobli.kafka.mirror

import java.util.Properties

import scala.collection.JavaConverters._

import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.internals.Topic


/**
  * Topics manipulation for testing propose
  */
trait TopicAdministration {

  val zkClientSrc: KafkaZkClient
  val kafkaClientDst: AdminClient

  /** AdminZkClient is flagged as "internal class"
    * using it through this implicit for further changes
    *
    * @see kafka.zk.AdminZkClient
    * @param zkClientSrc Kafka connection through zookeeper
    */
  implicit class AdminZkClientAdapter(zkClientSrc: KafkaZkClient) {

    private val adminZkClient = new AdminZkClient(zkClientSrc)

    implicit class MapToProperties(map: Map[String, String]) {
      def toProperties: Properties = {
        val configAsProps = new Properties()
        map.foreach { case (key, value) => configAsProps.setProperty(key, value) }
        configAsProps
      }
    }

    def createTopic(name: String, numPartitions: Int, replicationFactor: Int, config: Map[String, String] = Map.empty): Unit = {
      adminZkClient.createTopic(name, numPartitions, replicationFactor, config.toProperties)
    }

    def alterTopicConfig(name: String, config: Map[String, String]): Unit = {
      adminZkClient.changeTopicConfig(name, config.toProperties)
    }
  }

  def removeAllTopics(): Unit = {
    removeAllSrcTopics()
    removeAllDstTopics()
  }

  def removeAllSrcTopics(): Unit = {
    zkClientSrc.getAllTopicsInCluster.foreach(topic => {
      removeSrcTopic(topic)
    }
    )
  }

  def removeSrcTopic(topic: String): Unit = {
    if (!Topic.isInternal(topic)) {
      zkClientSrc.createDeleteTopicPath(topic)
      while (zkClientSrc.topicExists(topic)) Thread.sleep(500)
    }
  }

  def removeAllDstTopics(): Unit = {
    getDstTopicList.foreach(
      removeDstTopic
    )
  }

  def removeDstTopic(topic: String): Unit = if (!Topic.isInternal(topic)) kafkaClientDst.deleteTopics(Set(topic).asJava).all().get()

  def getDstTopicList: Set[String] = {
    kafkaClientDst.listTopics().names().get().asScala.toSet
  }

  def addSrcTopic(name: String, numPartitions: Int, replicationFactor: Int, config: Map[String, String] = Map.empty): Unit = {
    zkClientSrc.createTopic(name, numPartitions, replicationFactor, config)
  }

  def alterSrcTopicConfig(name: String, config: Map[String, String]): Unit = {
    zkClientSrc.alterTopicConfig(name, config)
  }

  def getDstTopic(name: String): Option[(Int, Int, Map[String, String])] = {
    val topicParam = Set(name).asJavaCollection
    val topicDescriptionOpt = kafkaClientDst.describeTopics(topicParam).all().get().asScala.get(name)
    topicDescriptionOpt match {
      case Some(topicDescription) =>
        val topicParam = Set(new ConfigResource(Type.TOPIC, topicDescription.name())).asJavaCollection
        val topicConfig = kafkaClientDst.describeConfigs(topicParam).all().get()
        val config = for {
          configs <- topicConfig.asScala.values
          entry <- configs.entries().asScala
        } yield entry.name() -> entry.value()

        Some(topicDescription.partitions().size(), topicDescription.partitions().iterator().next().replicas().size, config.toMap)
      case None => None
    }
  }
}