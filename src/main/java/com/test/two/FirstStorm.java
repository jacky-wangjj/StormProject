package com.test.two; /**
 * Created by wangjj17 on 2019/1/24.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class FirstStorm {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSpout());
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(false);
        if(args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("firststorm", conf, builder.createTopology());
            Utils.sleep(30000);
            cluster.killTopology("firststorm");
            cluster.shutdown();
        }
    }
}
