package in.dream_lab.genevents.boltsUidai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class FailBolt implements IRichBolt {
    OutputCollector _collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        BatchedFileLogging.writeToTemp(this, experiRunId);
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        System.out.println(tuple.getString(0) + " FailBolt");
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
