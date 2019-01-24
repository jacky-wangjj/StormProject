package com.test.three; /**
 * Created by wangjj17 on 2019/1/24.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    //public static class SplitSentence extends ShellBolt implements IRichBolt {
    public static class SplitSentence implements IRichBolt {
        private OutputCollector collector;
        private FileWriter fileWriter;
        String redis_config_path = "";

        public SplitSentence() {

        }

        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            redis_config_path = stormConf.get("redisCfg").toString();
            try {
                System.out.println("wangjj17:"+stormConf.get("localOutputPath").toString());
                this.fileWriter = new FileWriter(stormConf.get("localOutputPath").toString()); // 获取输出文件路径
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void execute(Tuple input) {
            String sentence = input.getString(0);
            String[] words = sentence.split("\t");
            for(String word : words){
                word = word.trim();
                if(!word.isEmpty()){
                    word = word.toLowerCase();
                    //Emit the word
                    List a = new ArrayList();
                    a.add(input);
                    System.out.println(redis_config_path + " - cur word:" + word);
                    try {
                        fileWriter.write(word + "\n"); // 写入输出文件
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    collector.emit(a,new Values(word));
                }
            }
            // Acknowledge the tuple
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void cleanup() {
            // TODO Auto-generated method stub
            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");

        for (int i = 0; i < args.length; ++i) {
            System.out.println("arg " + i + ":" + args[i]);
        }
        String topName = args[0];
        String runMode = args[1];
        String logFilePath = args[2];
        String readRedisConfig = args[3];
        String localOutputPath = args[4];
        Config conf = new Config();
        conf.put("logFile", logFilePath);
        conf.put("redisCfg", readRedisConfig);
        conf.put("localOutputPath", localOutputPath);
        conf.setDebug(true);
        System.out.println("Before Start...");
        //if (args != null && args.length > 0) {
        if (runMode.equalsIgnoreCase("srv")) { // 集群运行模式
            System.out.println("Server Mode...");
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else if (runMode.equalsIgnoreCase("loc")) { // 单机运行模式
            System.out.println("Local Mode...");
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        } else {
            System.out.println("Unknown Mode:" + runMode);
        }
    }
}
