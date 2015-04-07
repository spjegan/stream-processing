package com.twitter

import twitter4j._
import twitter4j.conf.ConfigurationBuilder

/**
 * Created by jegan on 6/4/15.
 */
object TwitterTest extends App {

  val configuration = new ConfigurationBuilder()
                      .setOAuthConsumerKey("$$$$$$")
                      .setOAuthConsumerSecret("$$$$$$")
                      .setOAuthAccessToken("$$$$$$")
                      .setOAuthAccessTokenSecret("$$$$$$").build()

  def statusListener = new StatusListener() {
    def onStatus(status: Status) { println(status.getText) }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { ex.printStackTrace }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }

  val twitterStream = new TwitterStreamFactory(configuration).getInstance
  twitterStream.addListener(statusListener)
//  twitterStream.filter(new FilterQuery(Array(1344951, 5988062, 807095, 3108351)))
  twitterStream.sample
  Thread.sleep(20000)
  twitterStream.cleanUp
  twitterStream.shutdown
}
