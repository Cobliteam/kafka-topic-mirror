
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

    def withKafkaClient(bootstrapServers: String)(f: TopicsReaderClient => Unit ): Unit = {
      try {
        val kafkaClient = new TopicsReaderClient(bootstrapServers)
        f(kafkaClient)
      } catch {
        case e: Throwable=> onException(e)
      }
    }

    withKafkaClient(opts.sourceBootstrapServers) {sourceClient => {
      withKafkaClient(opts.destinationBootstrapServers) { destinationClient => {
        try {
          val diffs = compareTopics(sourceClient.getTopicsInfo(), destinationClient.getTopicsInfo())
          for ((topic, diff) <- diffs) {
            println(s"$topic: ${diff.toString}")
          }
        } catch {
          case e: Throwable => onException(e)
        } finally {
          sourceClient.close()
          destinationClient.close()
          Exit.exit(exitCode)
        }
      }}
      sourceClient.close()
      Exit.exit(exitCode)
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
