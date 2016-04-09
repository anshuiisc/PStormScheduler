package in.dream_lab.genevents.boltsUidai;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.BioDedupCheckAPI;

import java.util.Map;

public class BioDedupCheckBolt implements IRichBolt {
    OutputCollector _collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        BioDedupCheckAPI.readFromXFS();
        BioDedupCheckAPI.packetDBDQuery();
        _collector.emit(tuple, new Values(tuple.getString(0) + " BioDedupCheckBolt"));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("packet"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}

