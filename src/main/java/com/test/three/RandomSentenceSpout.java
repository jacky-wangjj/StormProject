package com.test.three; /**
 * Created by wangjj17 on 2019/1/24.
 */
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;

//import storm.starter.hdfs.HdfsUtils;

public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    private FileReader fileReader;
    private boolean completed = false;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        try {
            String logFilePath = conf.get("logFile").toString();  // 获取数据源文件路径
            System.out.println("log_file_path:" + logFilePath);
            this.fileReader = new FileReader(logFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);

        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }
        String str;
        //Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        try{
            //Read all lines
            while((str = reader.readLine())!= null){
                /**
                 * By each line emmit a new value with the line as a their
                 */
                System.out.println("Spout Emit:" + str);
                //this._collector.emit(new Values(str),str);
                this._collector.emit(new Values(str)); // 读文件，并发送
            }
        } catch(Exception e){
            throw new RuntimeException("Errorreading tuple",e);
        }finally{
            completed = true;
        }
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}