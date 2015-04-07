package com.samples.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Created by Jegan on 4/6/2015.
 */
public class WordReaderSpout implements IRichSpout {

    private TopologyContext topologyContext;

    private SpoutOutputCollector collector;

    private Reader reader;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.topologyContext = topologyContext;
        this.collector = collector;
        try {
            this.reader = new FileReader(conf.get("wordsfile").toString());
        } catch (FileNotFoundException ex) {
            System.err.println("Unable to locate the file");
            ex.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException ex) {
            System.err.println("Unable to close the file");
            ex.printStackTrace();
        }
    }

    @Override
    public void activate() {
        System.out.println("Inside activate");
    }

    @Override
    public void deactivate() {
        System.out.println("Inside de-activate");
    }

    @Override
    public void nextTuple() {
        BufferedReader br = new BufferedReader(reader);
        String line = null;
/*        boolean completed = false;
        if (completed) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
        try {
            while((line = br.readLine()) != null) {
                this.collector.emit(new Values(line), line);
            }
        } catch (IOException ex) {
            System.err.println("Unable to read the file contents");
            ex.printStackTrace();
        }/* finally {
            completed = true;
        }*/
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("Message " + msgId + " is acknowledged");
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("Message " + msgId + " failed");
    }
}
