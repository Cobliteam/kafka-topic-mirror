
package co.cobli.kafka.mirror

import java.util

import scala.collection.JavaConverters._

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, Config, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.internals.Topic


/**
  * Topics manipulation for testing propose
  */
trait KafkasTopicsAdmin {

  val bootstrapServersSrc = "localhost:9092"
  val bootstrapServersDst = "localhost:29092"

  private def createKafkaClient(bootstrapServers: String): AdminClient ={
    val config = Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava
    AdminClient.create(config)
  }

  private val kafkaClientSrc = createKafkaClient(bootstrapServersSrc)
  private val kafkaClientDst = createKafkaClient(bootstrapServersDst)

  private implicit class AdminClientAdapter(kafkaClient: AdminClient) {

    def removeTopic(topic: String): Unit ={
      if (!Topic.isInternal(topic))
        kafkaClient.deleteTopics(Set(topic).asJava).all().get()
    }

    def getTopicList: Set[String] = {
      kafkaClient.listTopics().names().get().asScala.toSet
    }

    def removeAllTopics() = {
      getTopicList.foreach(
        removeTopic(_)
      )
    }

    def addTopic(name: String, numPartitions: Int, replicationFactor: Int, config: Map[String, String] = Map.empty): Unit = {
      val newTopic = new NewTopic(name, numPartitions, replicationFactor.toShort)
      if(!config.isEmpty){
        newTopic.configs(config.asJava)
      }
      kafkaClient.createTopics(Set(newTopic).asJava).all().get
    }

    def alterTopicConfig(name: String, config: Map[String, String]): Unit = {
      val newConfig = new Config(
        config.map { case (key, value) => new ConfigEntry(key, value) }.asJavaCollection
      )

      val alterParam = new util.HashMap[ConfigResource, Config] {
        put(new ConfigResource(ConfigResource.Type.TOPIC, name), newConfig)
      }

      //noinspection ScalaDeprecation
      kafkaClient.alterConfigs(alterParam).all.get
    }


  }

  def removeAllTopics(): Unit = {
    kafkaClientSrc.removeAllTopics()
    kafkaClientDst.removeAllTopics()
  }

  def getDstTopicList: Set[String] = {
    kafkaClientDst.getTopicList
  }

  def addSrcTopic(name: String, numPartitions: Int, replicationFactor: Int, config: Map[String, String] = Map.empty): Unit = {
    kafkaClientSrc.addTopic(name, numPartitions, replicationFactor, config)
  }

  def alterSrcTopicConfig(name: String, config: Map[String, String]): Unit = {
    kafkaClientSrc.alterTopicConfig(name, config)
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