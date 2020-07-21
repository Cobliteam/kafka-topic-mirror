
package co.cobli.kafka.mirror

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.admin.NewTopic


trait TopicsComparator {

  /**
    * Compare a list of topics with its associated TopicInfo
    * @param topicsInfoSrc source topics
    * @param topicsInfoDst destination topics
    * @return a map of TopicDiffs
    */
  def compareTopics(topicsInfoSrc: Map[String, TopicInfo], topicsInfoDst: Map[String, TopicInfo]): Map[String, TopicDiff] = {

    val diff: mutable.Map[String, TopicDiff] = mutable.Map.empty

    for ((topic, info) <- topicsInfoSrc) {
      if (topicsInfoDst.contains(topic)) {
        val infoDst = topicsInfoDst(topic)
        val diffPartitions = info.numPartitions - infoDst.numPartitions
        val diffReplicationFactor = info.replicationFactor - infoDst.replicationFactor
        val config = if (info.config.equals(infoDst)) {
          None
        } else {
          Some(info.config)
        }
        if (diffPartitions == 0 && diffReplicationFactor == 0 && config.isEmpty)
          diff += topic -> TopicDiff(TopicDiffType.EQUALS, 0, 0, None)
        else
          diff += topic -> TopicDiff(TopicDiffType.DIFFER, diffPartitions, diffReplicationFactor, config)
      } else {
        diff += topic -> TopicDiff(TopicDiffType.MISSING, info.numPartitions, info.replicationFactor, Some(info.config))
      }
    }
    diff.toMap
  }

  case class TopicDiff(diffType: TopicDiffType.Value, diffPartition: Int = 0, diffReplicationFactor: Int = 0, config: Option[Map[String, String]]) {

    import TopicDiffType._


    def asNewTopic(name: String): NewTopic = {
      val newTopic = new NewTopic(name, diffPartition, diffReplicationFactor.toShort)
      config match {
        case Some(conf) => newTopic.configs(conf.asJava)
        case _ => newTopic
      }
    }

    def needsManuallyIntervention(): Boolean = diffPartition != 0 || diffReplicationFactor != 0

    override def toString: String = {
      {
        if (diffType == MISSING) {
          "needs create with "
        } else if (diffType == DIFFER) {
          "needs update with "
        } else if (diffType == EQUALS) {
          "is equals "
        }
      } +
        f"partitions: $diffPartition%+d replication factor: $diffReplicationFactor%+d" + {
        config match {
          case None => ""
          case Some(conf) => s" config: $conf"
        }
      }
    }
  }

  object TopicDiffType extends Enumeration {
    type DiffType = Value
    val MISSING, DIFFER, EQUALS = Value
  }

}
