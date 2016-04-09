package in.dream_lab.genevents.boltsUidai;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.api.QualityCheckAPI;
import in.dream_lab.genevents.logging.BatchedFileLogging;

import java.util.Map;

public class QualityCheckBolt implements IRichBolt {
    private final String experiRunId;
    OutputCollector _collector;
    int count_dedup=1;

    public QualityCheckBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        QualityCheckAPI.qualityCheck();

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);
//        _collector.emit(new Values(rowString, msgId));

        if(count_dedup<96) {
            System.out.println("count_dedup QualityCheckBolt-"+count_dedup);
            _collector.emit(new Values(rowString, msgId));     count_dedup++;
        }

        else if(count_dedup>=96 && count_dedup <100)
        {
//         while(count_dedup<101)
             count_dedup++;

        }
        else
            count_dedup=1;



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

