package in.dream_lab.genevents.samples;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;


public class Spout extends BaseRichSpout {
    SpoutOutputCollector _collector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector= spoutOutputCollector;
    }

    public void nextTuple() {
        // TODO Read packet and forward to next bolt
        _collector.emit(new Values("Spout"));
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("packet"));
    }
}