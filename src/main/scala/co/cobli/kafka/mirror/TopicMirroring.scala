
package co.cobli.kafka.mirror


trait TopicMirroring extends TopicsComparing {

  /**
    * Mirror a specific topic based on a TopicDiff
    * @param name
    * @param diff
    * @param dst
    */
  def mirrorTopic(name: String, diff: TopicDiff, dst: TopicsManaging): Unit = {
    diff.diffType match {
      case TopicDiffType.MISSING =>
        println(s"creating topic $name")
        dst.createTopic(diff.asNewTopic(name))
      case TopicDiffType.DIFFER =>
        diff.config match {
          case Some(conf) =>
            println(s"updating topic $name config with $conf")
            dst.alterTopicConfig(name, conf)
          case None => ()
        }
        if (diff.needsManuallyIntervention()) System.err.println(s"$name needs manually intervention for $diff")
      case TopicDiffType.EQUALS =>
        println(s"nothing to be done for $name")
    }
  }
}