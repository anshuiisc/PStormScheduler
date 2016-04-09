package in.dream_lab.genevents.boltsUidai;

//import api.ManualDedupCheckAPI;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.ManualDedupCheckAPI;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.util.Map;

public class ManualDedupCheckBolt implements IRichBolt {
    private final String experiRunId;
    OutputCollector _collector;

    public ManualDedupCheckBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        ManualDedupCheckAPI.manualDedup();

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);
        _collector.emit(new Values(rowString, msgId));

//        _collector.emit(tuple, new Values(tuple.getString(0) + " ManualDedupCheckBolt"));
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
