package in.dream_lab.genevents.boltsUidai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.logging.BatchedFileLogging;
import in.dream_lab.genevents.utils.GlobalConstants;
import test.java.operation.Operations;

import java.util.Map;

public class IdentityFloat extends BaseRichBolt {


    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public IdentityFloat(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }




    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


        System.out.println("topo-PLUG-name-is-"+topologyContext.getRawTopology().toString());

//        String s="/Users/anshushukla/data/output/temp/spout-IdentityTopology-PLUG-123-0.1-Anshus-MacBook-Pro.local.log";
        BatchedFileLogging.writeToTemp(this, csvFileNameOutSink);



        this.collector=outputCollector;
        GlobalConstants.createBoltIdentifyingFiles(topologyContext);
    }

    @Override
    public void execute(Tuple input) {
        long ts1=System.currentTimeMillis(); //addon
        String rowString = input.getString(0);
        String msgId = input.getString(input.size()-1);
        Operations.doFloatOp(10);
        long ts2=System.currentTimeMillis(); //addon
        collector.emit(new Values(rowString,msgId,String.valueOf(ts2-ts1)));  //addon
// collector.emit(new Values(rowString,msgId ));
        System.out.println("CHECK:"+String.valueOf(ts2-ts1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Column","MSGID","time"));
    }
}