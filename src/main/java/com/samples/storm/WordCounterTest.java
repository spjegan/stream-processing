package com.samples.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by Jegan on 4/6/2015.
 */
public class WordCounterTest {

    public static void main(String s[]) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-normalizer", new WordNormaliserBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-normalizer");

        Config conf = new Config();
//        conf.put("wordsFile", "C:\\wsidea\\stream-processing\\storm-samples\\src\\main\\resources\\words.txt");
        conf.put("wordsFile", "/home/jegan/ws/stream-processing/src/main/resources/words.txt");
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogy", conf,
                builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();
    }
}
