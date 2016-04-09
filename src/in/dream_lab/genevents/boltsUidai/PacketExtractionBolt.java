package in.dream_lab.genevents.boltsUidai;


//import api.PacketExtractionAPI;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.PacketExtractionAPI;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.util.Map;

public class PacketExtractionBolt implements IRichBolt {
    private final String experiRunId;
    OutputCollector _collector;

    public PacketExtractionBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        PacketExtractionAPI.packetExtraction();
//        PacketExtractionAPI.saveToMySQl();
//        PacketExtractionAPI.saveToSOLR();
//        PacketExtractionAPI.saveToXFS();
//        PacketExtractionAPI.replicateToDC();

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);
        _collector.emit(new Values(rowString,msgId ));

//        _collector.emit(tuple, new Values(tuple.getString(0) + " PacketExtractionBolt"));
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
