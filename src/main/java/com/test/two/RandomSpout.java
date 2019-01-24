package com.test.two; /**
 * Created by wangjj17 on 2019/1/24.
 */
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};


    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));

    }

    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
        this.collector = arg2;
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("randomstring"));
    }

}
