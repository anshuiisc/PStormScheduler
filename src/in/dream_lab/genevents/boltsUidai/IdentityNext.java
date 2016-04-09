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

import java.util.Map;

public class IdentityNext extends BaseRichBolt {


    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public IdentityNext(String csvFileNameOutSink){
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
    	String rowString = input.getString(0)+"Next";
    	String msgId = input.getString(input.size()-1);    	
        collector.emit(new Values(rowString,msgId ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Column","MSGID"));
    }
}
