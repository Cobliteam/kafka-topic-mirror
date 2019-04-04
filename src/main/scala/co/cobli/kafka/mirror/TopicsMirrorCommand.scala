
package co.cobli.kafka.mirror

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection._

import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSet, OptionSpec, OptionSpecBuilder}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.utils.{Time, Utils}

import co.cobli.kafka.mirror.KafkaClientImplicits._


object TopicsMirrorCommand extends Logging with TopicMirroring {

  def main(args: Array[String]): Unit = {

    val opts = new TopicsMetadataUpsertCommandOptions(args)

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Diff or mirror topics between Kafkas.")

    // should have exactly one action
    val actions = Seq(opts.diffOpt, opts.mirrorOpt, opts.helpOpt).count(opts.options.has)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --diff or --mirror")

    opts.checkArgs()

    val zkClientSrc = KafkaZkClient(
      connectString = opts.options.valueOf(opts.zkConnectOptSrc),
      isSecure = false,
      sessionTimeoutMs = 10000,
      connectionTimeoutMs = 10000,
      maxInFlightRequests = 10000,
      time = Time.SYSTEM
    )

    opts.configPropsDst.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.kafkaConnectOptDst))

    val kafkaClientDst = AdminClient.create(opts.configPropsDst)
    var exitCode = 0
    val diffs = compareTopics(zkClientSrc.getTopicsInfo(), kafkaClientDst.getTopicsInfo())
    try {
      for ((topic, diff) <- diffs) {
        if (opts.options.has(opts.mirrorOpt)) {
          mirrorTopic(topic, diff, kafkaClientDst)
        }
      }
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      zkClientSrc.close()
      kafkaClientDst.close()
      Exit.exit(exitCode)
    }
  }

  class TopicsMetadataUpsertCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val zkConnectOptSrc: ArgumentAcceptingOptionSpec[String] = parser.accepts("zookeeper-src", "REQUIRED: The connection string for the source zookeeper connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    val kafkaConnectOptDst: ArgumentAcceptingOptionSpec[String] = parser.accepts("bootstrap-servers-dst", "REQUIRED: The connection string for the destination kafka connection in the form " +
      "host:port.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])

    val commandConfigPropertyOptDst: ArgumentAcceptingOptionSpec[String] = parser.accepts("command-config-property-dst", "A mechanism to pass user-defined properties in the form " +
      "key=value to the destination kafka Admin Client connection. Multiple entries allowed.")
      .withRequiredArg
      .describedAs("dst_prop")
      .ofType(classOf[String])

    val diffOpt: OptionSpecBuilder = parser.accepts("diff", "List topics differences.")
    val mirrorOpt: OptionSpecBuilder = parser.accepts("mirror", "Change or create topics on destination")
    val helpOpt: OptionSpecBuilder = parser.accepts("help", "Print usage information.")

    val options: OptionSet = parser.parse(args: _*)

    val configPropsDst: Properties = CommandLineUtils.parseKeyValueArgs(options.valuesOf(commandConfigPropertyOptDst).asScala)

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(diffOpt, mirrorOpt, helpOpt)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOptSrc)
      CommandLineUtils.checkRequiredArgs(parser, options, kafkaConnectOptDst)
    }
  }

}
