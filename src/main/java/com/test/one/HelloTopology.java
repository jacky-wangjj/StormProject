package com.test.one; /**
 * Created by wangjj17 on 2019/1/24.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;

public class HelloTopology {

    public static class HelloSpout extends BaseRichSpout {

        boolean isDistributed;
        SpoutOutputCollector collector;

        public HelloSpout() {
            this(true);
        }

        public HelloSpout(boolean isDistributed) {
            this.isDistributed = isDistributed;
        }

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public void close() {
        }

        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[] {"china", "usa", "japan", "russia", "england"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            this.collector.emit(new Values(word));
        }

        public void ack(Object msgId) {
        }

        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if(!this.isDistributed) {
                Map<String, Object> ret = new TreeMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            } else {
                return null;
            }
        }
    }

    public static class HelloBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            this.collector.emit(tuple, new Values("hello," + tuple.getString(0)));
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("a", new HelloSpout(), 10);
        builder.setBolt("b", new HelloBolt(), 5).shuffleGrouping("a");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            String test_id = "hello_test";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(test_id, conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(test_id);
            cluster.shutdown();
        }
    }
}
