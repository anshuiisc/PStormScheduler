package in.dream_lab.genevents.boltsUidai;


//import api.DemoDedupCheckAPI;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.DemoDedupCheckAPI;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.util.Map;

public class DemoDedupCheckBolt implements IRichBolt {
    private final String experiRunId;
    OutputCollector _collector;
    int count_dedup=1;

    public DemoDedupCheckBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);
        _collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        DemoDedupCheckAPI.DedupCheck();


        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);

//        _collector.emit(new Values(rowString, msgId));

        if(count_dedup<70)
        { System.out.println("value fo count_dedup-"+count_dedup);
            _collector.emit(new Values(rowString, msgId));     count_dedup++;
        }

        else
         count_dedup=1;



//        _collector.emit(tuple, new Values(tuple.getString(0) + " DemoDedupCheckBolt"));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Column","MSGID"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
