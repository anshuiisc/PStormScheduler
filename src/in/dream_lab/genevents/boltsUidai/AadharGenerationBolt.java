package in.dream_lab.genevents.boltsUidai;

//import api.AadharGenerationAPI;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.AadharGenerationAPI;
import in.dream_lab.genevents.config.LatencyConfig;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.util.Map;

public class AadharGenerationBolt implements IRichBolt {
    String experiRunId;
    OutputCollector _collector;
    String fileContent=null;
    public AadharGenerationBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);

         fileContent=LatencyConfig.readFileforOp();

        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        AadharGenerationAPI.getAadharNo(fileContent);
//        AadharGenerationAPI.saveAadharData();
//        AadharGenerationAPI.printAadharCard();
//        AadharGenerationAPI.dispatchAadharCard();
        System.out.println(tuple.getString(0) + " AadharBolt");

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);
        _collector.emit(new Values(rowString, msgId));



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
