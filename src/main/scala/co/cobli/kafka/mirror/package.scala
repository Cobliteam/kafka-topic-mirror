
package co.cobli.kafka.mirror

import org.apache.kafka.clients.admin.NewTopic


trait TopicsReading {
  def getNonInternalTopics: Set[String]

  def getTopicsInfo(topics: Set[String] = getNonInternalTopics): Map[String, TopicInfo]
}

trait TopicsWriting {
  def createTopic(newTopic: NewTopic)

  def alterTopicConfig(topic: String, config: Map[String, String])
}

trait TopicsManaging extends TopicsReading with TopicsWriting