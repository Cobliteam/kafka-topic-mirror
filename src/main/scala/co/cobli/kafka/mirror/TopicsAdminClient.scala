
package co.cobli.kafka.mirror

import java.util

import scala.collection.JavaConverters._

import org.apache.kafka.clients.admin.{Config, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource


class TopicsAdminClient(bootstrapServers: String) extends TopicsReaderClient(bootstrapServers) {

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

    //noinspection ScalaDeprecation
    client.alterConfigs(alterParam).all.get
  }

}
