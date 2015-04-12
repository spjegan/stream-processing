package com.twitter.kafka

import akka.actor.Actor
import com.samples.kafka.Producer

/**
 * Created by jegan on 12/4/15.
 */
class KafkaStreamer extends Actor {

  val brokers = "localhost:9091,localhost:9092,localhost:9093,localhost:9094"

  val producer = new Producer(brokers, "tweets")

  override def receive: Receive = {
    case KafkaMessage(message) => producer.send(message)
    case _ => Console.err.println("Unknown message received.")
  }
}

case class KafkaMessage(message: String)
