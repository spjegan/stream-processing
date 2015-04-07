package com.samples.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Jegan on 4/6/2015.
 */
public class WordNormaliserBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("WordNormaliserBolt - Inside prepare");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("WordNormaliserBolt - Inside execute");
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                //Emit the word
                List<Tuple> tuples = new ArrayList<>();
                tuples.add(input);
                collector.emit(tuples, new Values(word));
            }
        }
        // Acknowledge the tuple
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        System.out.println("WordNormaliserBolt - Inside cleanup");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println("WordNormaliserBolt - Inside declareOutputFields");
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        System.out.println("WordNormaliserBolt - Inside getComponentConfiguration");
        return null;
    }
}
