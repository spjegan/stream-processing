package com.twitter

import akka.actor.{Props, ActorSystem}
import com.twitter.kafka.{KafkaMessage, KafkaStreamer}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

/**
 * Created by jegan on 6/4/15.
 */
object TwitterTest extends App {

  val configuration = new ConfigurationBuilder()
                      .setDebugEnabled(true)
                      .setOAuthConsumerKey("cZuOtLLtdpPe2vaiF5mZQxZIB")
                      .setOAuthConsumerSecret("eePBISVmXBWhk01uvb810qi5IpIo3xnIgJmbhdywqarV3dlmYL")
                      .setOAuthAccessToken("1394959760-IsZz7rGXxAuVox72rTAm8BomskV4VeoAg7tOi0y")
                      .setOAuthAccessTokenSecret("YXAiw4sKyqkDSjmBh9VaY1OEPRkMqSipx6Oa5PdtD0h8o")
                      .build()

  val as = ActorSystem("StreamingActors")
  val streamer = as.actorOf(Props[KafkaStreamer], name = "KafkaStreamer")

  def statusListener = new StatusListener() {
    def onStatus(status: Status) = {
      println(status.getText)
      streamer ! KafkaMessage(status.getText)
    }

    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = ???

    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) = ???

    def onException(ex: Exception) = ex.printStackTrace

    override def onStallWarning(warning: StallWarning): Unit = ???

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ???
  }

  val twitterStream = new TwitterStreamFactory(configuration).getInstance
  twitterStream.addListener(statusListener)
  twitterStream.sample("en")
}
