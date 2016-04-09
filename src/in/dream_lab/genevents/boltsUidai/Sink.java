package in.dream_lab.genevents.boltsUidai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created by anshushukla on 19/05/15.
 */
public class Sink extends BaseRichBolt {

    OutputCollector collector;
    BatchedFileLogging ba;
    String csvFileNameOutSink;  //Full path name of the file at the sink bolt

    public Sink(){

    }

    public Sink(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
         //ba=new BatchedFileLogging();
        ba=new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId());

        System.out.println("SinkBolt PID,"+ ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("MSGID");
        String exe_time = input.getStringByField("time");  //addon
        System.out.println("exe_time-"+exe_time);
        //collector.emit(input,new Values(msgId));
        try {
 //        ba.batchLogwriter(System.nanoTime(),msgId);
 ba.batchLogwriter(System.currentTimeMillis(),msgId+","+exe_time);//addon
        } catch (Exception e) {
            e.printStackTrace();
        }
        //collector.ack(input);
    }


//    @Override
//    public void execute(Tuple input) {
////input.ge
//        String sentence = input.getString(0);
//        collector.emit(input,new Values(sentence ));
//        //System.out.println("===================sink=="+sentence+"$$$$$$$$$");
//        try {
//            ba.batchLogwriter(System.nanoTime(),sentence);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
////        collector.ack(tuple);
//        collector.ack(input);
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
