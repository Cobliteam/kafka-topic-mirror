
package co.cobli.kafka.mirror

import java.io.ByteArrayOutputStream

import scala.collection.mutable
import scala.util.Random

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FeatureSpec, GivenWhenThen}


class TopicsMirrorCommandSpec extends FeatureSpec with GivenWhenThen with KafkasTopicsAdmin with SystemExitInterceptor with BeforeAndAfter
  with BeforeAndAfterAll {


  before {
    removeAllTopics()
  }

  override protected def beforeAll(): Unit = setupSystemExitInterceptor()

  override protected def afterAll(): Unit = dismissSystemExitInterceptor()

  info("As a Kafka administrator")
  info("I want to be able to mirror kafka topics from a kafka to another")

  feature("Mirror kafka topics") {
    scenario("Both kafkas are empty") {
      Given("that kafka source has no topic")

      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring()

      Then("should return successful result")
      assert(status == 0)

      And("not create anything on destination")
      assert(getDstTopicList.isEmpty)
    }

    scenario("Source kafka has one topic") {
      Given("that kafka source has one topic")
      val (name, numPartitions, replicationFactor, _) = generateRandomTopic()

      When("topic mirror is run in dry-run mode")
      val (dryRunStatus, _, _) = executeKafkaMirroring(dryRun = true)

      Then("should return successful result")
      assert(dryRunStatus == 0)

      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring()

      Then("should return successful result")
      assert(status == 0)

      And("create a single topic on destination")
      assertResult(Set(name)) {
        getDstTopicList
      }

      assertDestinationTopicAttributes(name, numPartitions, replicationFactor, Map.empty)
    }

    scenario("Source kafka has one topic with config entries") {
      Given("that kafka source has one topic with config entries")
      val (name, numPartitions, replicationFactor, config) = generateRandomTopic(generateRandomConfig())

      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring()

      Then("should return successful result")
      assert(status == 0)

      And("create a single topic on destination")
      assertResult(Set(name)) {
        getDstTopicList
      }

      assertDestinationTopicAttributes(name, numPartitions, replicationFactor, config)
    }

    scenario("Source kafka has 2 or more topic with and without config entries") {
      Given("that kafka source has 2 or more topic with and without config entries")
      val topics: mutable.Set[(String, Int, Int, Map[String, String])] = mutable.Set.empty

      (0 to 1 + Random.nextInt(3)).foreach(_ => {
        topics += generateRandomTopic()
      })
      (0 to 1 + Random.nextInt(3)).foreach(_ => {
        topics += generateRandomTopic(generateRandomConfig())
      })

      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring()

      Then("should return successful result")
      assert(status == 0)

      And("create all topics on destination")
      assert(topics.map(_._1) == getDstTopicList)

      And("with all equivalent attributes")
      topics.foreach {
        case (name, numPartitions, replicationFactor, config) => assertDestinationTopicAttributes(name, numPartitions, replicationFactor, config)
      }
    }

    scenario("Mirror configuration parameters of topics") {
      Given("that kafka source and destiny has 2 mirrored topic ")
      val (name1, numPartitions1, replicationFactor1, _) = generateRandomTopic()
      val (name2, numPartitions2, replicationFactor2, _) = generateRandomTopic(generateRandomConfig())
      executeKafkaMirroring()

      When("source kafka topics change its configuration")
      val config1 = generateRandomConfig()
      val config2 = generateRandomConfig()
      alterSrcTopicConfig(name1, config1)
      alterSrcTopicConfig(name2, config2)

      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring()

      Then("should return successful result")
      assert(status == 0)

      And("change topics configuration on destination")

      assertDestinationTopicAttributes(name1, numPartitions1, replicationFactor1, config1)
      assertDestinationTopicAttributes(name2, numPartitions2, replicationFactor2, config2)
    }

    def generateRandomTopic(config: Map[String, String] = Map.empty): (String, Int, Int, Map[String, String]) = {
      val numPartitions = 1 + Random.nextInt(10)
      //val replicationFactor = 1 + Random.nextInt(3);
      val replicationFactor = 1
      val name = "test_topic_" + (Random.alphanumeric take 10).mkString
      addSrcTopic(name, numPartitions, replicationFactor, config)
      (name, numPartitions, replicationFactor, config)
    }

    def generateRandomConfig(): Map[String, String] = {
      Map(
        "delete.retention.ms" -> (80000000 + Random.nextInt(6400000)).toString,
        "min.cleanable.dirty.ratio" -> (0.1 + Random.nextDouble() / 2).toString,
        "compression.type" -> Random.shuffle(List("uncompressed", "gzip")).head
      )
    }

    def assertDestinationTopicAttributes(name: String, numPartitions: Int, replicationFactor: Int, config: Map[String, String]) {

      val topicOpt = getDstTopic(name)

      And(name + " must exists")
      assert(topicOpt.nonEmpty)
      val topic = topicOpt.get

      And(name + " must have same number of partitions")
      assertResult(numPartitions) {
        topic._1
      }

      And(name + " must have same replication factor")
      assertResult(replicationFactor) {
        topic._2
      }

      And(name + " must contains same configuration entries")
      config.foreach {
        case (key, expected) =>
          assert(topic._3.get(key).isDefined)
          val got = topic._3(key)
          assertResult(expected, s"on topic $name in config entry $key") {
            got
          }
      }
    }
  }

  feature("Exit nicely on errors") {
    scenario("Kafka is down") {
      Given("that Kafka does not exists (hostname not found)")
      When("topic mirror is run")
      val (status, _, _) = executeKafkaMirroring(dstConn = "unreachable:9092")
      Then("status code must be equals 1")
      assert(status == 1)
    }
  }


  def executeKafkaMirroring(srcConn: String = bootstrapServersSrc, dstConn: String = bootstrapServersDst, dryRun: Boolean = false): (Int, String, String) = {
    var status = 0
    val stream = new ByteArrayOutputStream()
    val streamErr = new ByteArrayOutputStream()

    def logAndReturn: (Int, String, String) ={
      info(stream.toString())
      info(streamErr.toString())
      (status, stream.toString(), streamErr.toString())
    }


    Console.withOut(stream) {
      Console.withErr(streamErr) {
        try {
          TopicsMirrorCommand.main(mirrorCommand(srcConn, dstConn, dryRun))
        } catch {
          case e: ExitException =>
            status = e.status
            return logAndReturn
        }
      }
    }
    logAndReturn
  }


  def mirrorCommand(srcConn: String = bootstrapServersSrc, dstConn: String = bootstrapServersDst, dryRun: Boolean): Array[String] = {
    val parameters = Array(
      "--bootstrap-servers-src", srcConn,
      "--bootstrap-servers-dst", dstConn
    )
    dryRun match {
      case true => parameters :+ "--dry-run"
      case false => parameters
    }
  }

}
