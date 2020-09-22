package com.kdg.kafka

import java.lang
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig, Topology}

import scala.util.control.Breaks

object FavouriteColorApp {

  def createConfig(): Properties = {

    val config = new Properties

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Disable the Cache to demonstrate all the "steps" involved in the transformation - Not Recommended in PROD
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    config
  }

  def createTopology(): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    val usersColorsInput: KStream[String, String] = builder.stream[String, String]("users-colors-input")
    val usersColorsOutput: KStream[String, String] = usersColorsInput
      .filter((user: String, color: String) => !user.trim.isEmpty && !color.trim.isEmpty)
      //.map((user: String, color: String) => new KeyValue[String, String](user.trim.toLowerCase, color.trim.toLowerCase))
      .selectKey((user: String, _) => user.trim.toLowerCase())
      .mapValues(_.trim.toLowerCase)
      .filter((user: String, color: String) => List("green", "red", "blue").contains(color)) // Select only records with given colors.
    usersColorsOutput.to("users-colors-output")

    val usersColorsOut: KTable[String, String] = builder.table("users-colors-output")
    val favouriteColours: KTable[String, lang.Long] = usersColorsOut
      //.filter((user: String, color: String) => List("green", "red", "blue").contains(color)) //Depend on Use-Case
      .groupBy((_, color: String) => new KeyValue[String, String](color, color))
      .count(
        Materialized.as[String, lang.Long, KeyValueStore[Bytes, Array[Byte]]]("CountsByColors")
          .withKeySerde(Serdes.String())
          .withValueSerde(Serdes.Long())
      )

    favouriteColours.toStream.to("favourite-colours", Produced.`with`(Serdes.String(), Serdes.Long()))

    builder.build()
  }

  def main(args: Array[String]): Unit = {

    // Create Stream
    val streams: KafkaStreams = new KafkaStreams(createTopology(), createConfig())

    // Only do this in DEV - Not in PROD
    streams.cleanUp()

    // Start Kafka Stream
    streams.start()

    // Shutdown the application gracefully
//    Runtime.getRuntime.addShutdownHook(new Thread{
//      override def run(): Unit = {
//        streams.close();
//      }
//    })
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }

    // Print the Topology
    System.out.println(streams.toString)

    // Learning Purpose - Print the Topology every 5 seconds
    while (true) {
      streams.localThreadsMetadata().forEach(t => System.out.println(t.toString));
      try {
        Thread.sleep(5000);
      } catch {
        case e: InterruptedException => Breaks.break()
      }
    }
  }
}
