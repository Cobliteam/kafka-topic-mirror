
package co.cobli.kafka.mirror

import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSet, OptionSpecBuilder}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.Utils

object TopicsMirrorCommand extends Logging with TopicsComparator {

  def main(args: Array[String]): Unit = {

    val opts = new TopicsMirrorCommandOptions(args)

    opts.checkArgs(args)

    var exitCode = 0

    def onException(e: Throwable): Unit ={
      println("Error while executing topic command : " + e.getMessage)
      error(Utils.stackTrace(e))
      exitCode = 1
    }

    def kafkaClientReaderFactory(bootstrapServers: String): TopicsReaderClient = { new TopicsReaderClient(bootstrapServers)}
    def kafkaClientAdminFactory(bootstrapServers: String): TopicsAdminClient = { new TopicsAdminClient(bootstrapServers)}

    def withKafkaClient[T](bootstrapServers: String, factory: String => T)(callback: T => Unit ): Unit = {
      try {
        val kafkaClient = factory(bootstrapServers)
        callback(kafkaClient)
      } catch {
        case e: Throwable=> onException(e)
      }
    }

    def mirrorTopic(name: String, diff: TopicDiff, destinationClient: TopicsAdminClient): Unit = {
      diff.diffType match {
        case TopicDiffType.MISSING =>
          println(s"creating topic $name")
          destinationClient.createTopic(diff.asNewTopic(name))
        case TopicDiffType.DIFFER =>
          diff.config match {
            case Some(conf) =>
              println(s"updating topic $name config with $conf")
              destinationClient.alterTopicConfig(name, conf)
            case None => ()
          }
          if (diff.needsManuallyIntervention()) System.err.println(s"$name needs manually intervention for $diff")
        case TopicDiffType.EQUALS =>
          println(s"nothing to be done for $name")
      }
    }

    withKafkaClient(opts.sourceBootstrapServers, kafkaClientReaderFactory) {sourceClient: TopicsReaderClient => {
      withKafkaClient(opts.destinationBootstrapServers, kafkaClientAdminFactory) { destinationClient: TopicsAdminClient => {
        try {
          val diffs = compareTopics(sourceClient.getTopicsInfo(), destinationClient.getTopicsInfo())
          for ((topic, diff) <- diffs) {
            if (opts.isDryRun)
              println(s"$topic: ${diff.toString}")
            else
              mirrorTopic(topic, diff, destinationClient)
          }
        } catch {
          case e: Throwable => onException(e)
        } finally {
          destinationClient.close()
        }
      }}
      sourceClient.close()
    }}
    Exit.exit(exitCode)
  }

  class TopicsMirrorCommandOptions(args: Array[String]) {
    private val parser = new OptionParser(false)

    private val bootstrapServersSrc: ArgumentAcceptingOptionSpec[String] = parser.accepts("bootstrap-servers-src", "REQUIRED: The " +
      "connection string for the source kafka connection in the form host:port.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])

    private val bootstrapServersDst: ArgumentAcceptingOptionSpec[String] = parser.accepts("bootstrap-servers-dst", "REQUIRED: The " +
      "connection string for the destination kafka connection in the form host:port.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])

    private val dryRunOpt: OptionSpecBuilder = parser.accepts("dry-run", "Just list topics differences without mirroring anything.")
    private val helpOpt: OptionSpecBuilder = parser.accepts("help", "Print usage information.")

    private val options: OptionSet = {
      try {
        parser.parse(args: _*)
      } catch {
        case e: Throwable => CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }

    def checkArgs(args: Array[String]) {
      // check required args
      if (args.length == 0 || isHelp)
        CommandLineUtils.printUsageAndDie(parser, "Mirror topics between Kafkas.")

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServersSrc, bootstrapServersDst)
    }

    def isHelp: Boolean = options.has(helpOpt)

    def isDryRun: Boolean = options.has(dryRunOpt)

    def sourceBootstrapServers: String = options.valueOf(bootstrapServersSrc)

    def destinationBootstrapServers: String = options.valueOf(bootstrapServersDst)

  }
}
